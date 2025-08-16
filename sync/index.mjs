// --- Sync Google Drive -> Supabase (Manuals) ---
import { GoogleAuth } from 'google-auth-library';
import { drive_v3, google } from 'googleapis';   // ✅ correct import
import { createClient } from '@supabase/supabase-js';
import CryptoJS from 'crypto-js';
import mime from 'mime-types';

// -------- Env checks --------
const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  DRIVE_ROOT_FOLDER_ID,
  GOOGLE_SERVICE_ACCOUNT_JSON
} = process.env;

function die(msg) {
  console.error('FATAL:', msg);
  process.exit(1);
}
if (!SUPABASE_URL) die('SUPABASE_URL is missing');
if (!SUPABASE_SERVICE_ROLE_KEY) die('SUPABASE_SERVICE_ROLE_KEY is missing');
if (!DRIVE_ROOT_FOLDER_ID) die('DRIVE_ROOT_FOLDER_ID is missing');
if (!GOOGLE_SERVICE_ACCOUNT_JSON) die('GOOGLE_SERVICE_ACCOUNT_JSON is missing');

console.log('Env OK. Starting sync…');

// -------- Clients --------
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

function parseServiceAccount() {
  let json;
  try {
    json = JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON);
  } catch (e) {
    die('GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON');
  }
  if (!json.client_email || !json.private_key) {
    die('Service account JSON missing client_email or private_key');
  }
  console.log('Service account:', json.client_email);
  return new GoogleAuth({
    credentials: json,
    scopes: ['https://www.googleapis.com/auth/drive.readonly']
  });
}

// -------- Helpers --------
function brandFromPath(path) {
  const parts = path.split('/').filter(Boolean);
  return parts.length >= 2 ? parts[1] : 'Unknown';
}

function categoryFromPath(path) {
  const first = path.split('/').filter(Boolean)[0] || '';
  if (/powersports/i.test(first)) return 'Powersports';
  if (/small engines?/i.test(first)) return 'Small Engines';
  return first || 'Uncategorized';
}

function titleFromName(name) {
  return name.replace(/\.[^.]+$/, '');
}

async function listAllFiles(drive, folderId, prefix = '') {
  const items = [];
  async function walk(id, pathPrefix) {
    let pageToken = undefined;
    do {
      const res = await drive.files.list({
        q: `'${id}' in parents and trashed=false`,
        fields: 'nextPageToken, files(id, name, mimeType)',
        pageToken
      });
      pageToken = res.data.nextPageToken || undefined;
      for (const f of res.data.files || []) {
        const path = pathPrefix ? `${pathPrefix}/${f.name}` : f.name;
        if (f.mimeType === 'application/vnd.google-apps.folder') {
          await walk(f.id, path);
        } else {
          items.push({ id: f.id, name: f.name, mimeType: f.mimeType, path });
        }
      }
    } while (pageToken);
  }
  await walk(folderId, prefix);
  return items;
}

async function downloadFileBytes(drive, fileId) {
  try {
    const res = await drive.files.get(
      { fileId, alt: 'media' },
      { responseType: 'arraybuffer' }
    );
    // Force Node Buffer
    return Buffer.from(new Uint8Array(res.data || []));
  } catch (e) {
    console.error('Download failed for fileId', fileId, e.message);
    return null;
  }
}

function md5(buffer) {
  const wordArray = CryptoJS.lib.WordArray.create(buffer);
  return CryptoJS.MD5(wordArray).toString();
}

async function uploadToStorage(buffer, destinationPath, contentType) {
  const { data, error } = await supabase.storage
    .from('manuals')
    .upload(destinationPath, buffer, { contentType, upsert: true });
  if (error) throw error;
  return data.path;
}

async function upsertManual(row) {
  const { data, error } = await supabase
    .from('manuals')
    .upsert(row, { onConflict: 'drive_file_id' })
    .select('id')
    .single();
  if (error) throw error;
  return data.id;
}

// -------- Main --------
async function run() {
  const auth = parseServiceAccount();
  const drive = google.drive({ version: 'v3', auth });
  const start = new Date();
  let files_scanned = 0;
  let files_changed = 0;

  console.log('Listing Drive files under root:', DRIVE_ROOT_FOLDER_ID);
  const files = await listAllFiles(drive, DRIVE_ROOT_FOLDER_ID);
  console.log(`Found ${files.length} files (including non-PDF).`);

  for (const f of files) {
    files_scanned++;

    // Skip Google-native files (Docs/Sheets/Slides) — add export later if desired
    if ((f.mimeType || '').startsWith('application/vnd.google-apps.')) {
      console.warn('Skipping Google-native file:', f.name, f.mimeType);
      continue;
    }

    const buf = await downloadFileBytes(drive, f.id);
    if (!buf || !Buffer.isBuffer(buf) || buf.length === 0) {
      console.warn('Skipping file (no bytes):', f.name, f.mimeType);
      continue;
    }

    const sum = md5(buf);
    const ext = mime.extension(f.mimeType) || f.name.split('.').pop() || 'bin';
    const storagePath = f.path; // mirror Drive path
    const category = categoryFromPath(f.path);
    const brand = brandFromPath(f.path);
    const title = titleFromName(f.name);

    const existing = await supabase
      .from('manuals')
      .select('id, checksum')
      .eq('drive_file_id', f.id)
      .maybeSingle();

    if (existing.data && existing.data.checksum === sum) {
      continue; // unchanged
    }

    // Upload original to Storage
    const storedPath = await uploadToStorage(
      buf,
      storagePath,
      f.mimeType || mime.lookup(ext) || 'application/octet-stream'
    );

    // Build JSON (no parsed content yet)
    const json = {
      category,
      brand,
      title,
      source: { provider: 'google_drive', fileId: f.id, path: f.path },
      tags: [],
      mimeType: f.mimeType,
      pages: undefined,
      content: { text: undefined }
    };

    await upsertManual({
      category,
      brand,
      title,
      source_provider: 'google_drive',
      drive_file_id: f.id,
      drive_path: f.path,
      mime_type: f.mimeType,
      checksum: sum,
      storage_path: storedPath,
      tags: [],
      parsed_ok: false,
      pages: null,
      extracted_text: null,
      json
    });

    files_changed++;
    if (files_changed % 10 === 0) {
      console.log(`Progress: ${files_changed} updated so far…`);
    }
  }

  await supabase.from('sync_log').insert({
    started_at: start.toISOString(),
    finished_at: new Date().toISOString(),
    files_scanned,
    files_changed,
    notes: 'cron run'
  });

  console.log(`Done. Scanned ${files_scanned}, updated ${files_changed}`);
}

run().catch(async (e) => {
  console.error('UNCAUGHT ERROR:', e.message);
  try {
    await supabase.from('sync_log').insert({
      started_at: new Date().toISOString(),
      finished_at: new Date().toISOString(),
      notes: `error: ${e.message}`
    });
  } catch {}
  process.exit(1);
});

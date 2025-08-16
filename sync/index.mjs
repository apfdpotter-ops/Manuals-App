import { GoogleAuth } from 'google-auth-library';
import pkg from '@googleapis/drive';
const { google } = pkg;
import { createClient } from '@supabase/supabase-js';
import pdf from 'pdf-parse';
import CryptoJS from 'crypto-js';
import mime from 'mime-types';

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  DRIVE_ROOT_FOLDER_ID,
  GOOGLE_SERVICE_ACCOUNT_JSON
} = process.env;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

function parseSA() {
  const json = JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON);
  return new GoogleAuth({
    credentials: json,
    scopes: ['https://www.googleapis.com/auth/drive.readonly']
  });
}

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
  const res = await drive.files.get(
    { fileId, alt: 'media' },
    { responseType: 'arraybuffer' }
  );
  return Buffer.from(res.data);
}

function md5(buffer) {
  const wordArray = CryptoJS.lib.WordArray.create(buffer);
  return CryptoJS.MD5(wordArray).toString();
}

async function parsePdf(buffer) {
  const data = await pdf(buffer);
  return { text: data.text || '', pages: data.numpages || null };
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

async function run() {
  const auth = parseSA();
  const drive = google.drive({ version: 'v3', auth });

  const start = new Date();
  let files_scanned = 0;
  let files_changed = 0;

  const files = await listAllFiles(drive, DRIVE_ROOT_FOLDER_ID);
  for (const f of files) {
    files_scanned++;

    const buf = await downloadFileBytes(drive, f.id);
    const sum = md5(buf);
    const ext = mime.extension(f.mimeType) || f.name.split('.').pop() || 'bin';
    const storagePath = f.path;
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

    const storedPath = await uploadToStorage(
      buf,
      storagePath,
      f.mimeType || mime.lookup(ext) || 'application/octet-stream'
    );

    let extracted_text = null;
    let pages = null;
    let parsed_ok = false;

    if ((f.mimeType && f.mimeType.includes('pdf')) || /\.pdf$/i.test(f.name)) {
      try {
        const parsed = await parsePdf(buf);
        extracted_text = parsed.text;
        pages = parsed.pages;
        parsed_ok = true;
      } catch {
        parsed_ok = false;
      }
    }

    const json = {
      category,
      brand,
      title,
      source: { provider: 'google_drive', fileId: f.id, path: f.path },
      tags: [],
      mimeType: f.mimeType,
      pages: pages || undefined,
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
      parsed_ok,
      pages,
      extracted_text,
      json
    });

    files_changed++;
  }

  await supabase.from('sync_log').insert({
    started_at: start.toISOString(),
    finished_at: new Date().toISOString(),
    files_scanned,
    files_changed,
    notes: 'cron run'
  });

  console.log(`Scanned ${files_scanned}, updated ${files_changed}`);
}

run().catch(async (e) => {
  console.error(e);
  try {
    await supabase.from('sync_log').insert({
      started_at: new Date().toISOString(),
      finished_at: new Date().toISOString(),
      notes: `error: ${e.message}`
    });
  } catch {}
  process.exit(1);
});

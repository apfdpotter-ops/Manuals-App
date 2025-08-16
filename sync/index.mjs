// --- imports ---
import { google } from 'googleapis';
import { createClient } from '@supabase/supabase-js';
import mime from 'mime-types';
import crypto from 'node:crypto';

// ---- tiny helpers ----
const die = (msg) => { console.error(msg); process.exit(1); };
const md5 = (buf) => crypto.createHash('md5').update(buf).digest('hex');
const now = () => new Date().toISOString();

// --- env ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const GOOGLE_SERVICE_ACCOUNT_JSON = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
const GOOGLE_DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID;
const SUPABASE_BUCKET = process.env.SUPABASE_BUCKET || 'manuals';
const MAX_UPLOAD_BYTES = parseInt(process.env.MAX_UPLOAD_BYTES || '52428800', 10); // 50MB

if (!SUPABASE_URL) die('SUPABASE_URL is missing');
if (!SUPABASE_SERVICE_ROLE_KEY) die('SUPABASE_SERVICE_ROLE_KEY is missing');
if (!GOOGLE_SERVICE_ACCOUNT_JSON) die('GOOGLE_SERVICE_ACCOUNT_JSON is missing');
if (!GOOGLE_DRIVE_FOLDER_ID) die('GOOGLE_DRIVE_FOLDER_ID is missing');

// --- clients ---
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Try to decode the JWT just for a friendly debug line (will always fail if accidentally a URL)
try {
  const parts = SUPABASE_SERVICE_ROLE_KEY.split('.');
  const payload = JSON.parse(Buffer.from(parts[1], 'base64').toString());
  console.log('SUPABASE KEY ROLE:', payload.role, 'iss:', payload.iss || 'supabase');
} catch {
  console.log('Could not decode SUPABASE key payload');
}

// Google Auth from JSON
function parseServiceAccount() {
  /** @type {Record<string, any>} */
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
  return new google.auth.GoogleAuth({
    credentials: json,
    scopes: ['https://www.googleapis.com/auth/drive.readonly'],
  });
}

// ------- Drive listing (recursive) -------
/**
 * Recursively list PDFs under a folder, returning array of
 * { id, name, mimeType, size, path }
 */
async function listDrivePdfs(drive, folderId, prefix = '') {
  const results = [];

  // list subfolders
  const listFolders = await drive.files.list({
    q: `'${folderId}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
    fields: 'files(id,name)',
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  for (const folder of listFolders.data.files || []) {
    const childPath = prefix ? `${prefix}/${folder.name}` : folder.name;
    const children = await listDrivePdfs(drive, folder.id, childPath);
    results.push(...children);
  }

  // list PDF files in this folder
  let pageToken = undefined;
  do {
    const resp = await drive.files.list({
      q: `'${folderId}' in parents and mimeType = 'application/pdf' and trashed = false`,
      fields: 'nextPageToken, files(id,name,mimeType,size)',
      pageToken,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });
    for (const f of resp.data.files || []) {
      const path = prefix ? `${prefix}/${f.name}` : f.name;
      results.push({
        id: f.id,
        name: f.name,
        mimeType: f.mimeType || 'application/pdf',
        size: Number(f.size || 0),
        path,
      });
    }
    pageToken = resp.data.nextPageToken || undefined;
  } while (pageToken);

  return results;
}

// ------- Download one Drive file to Buffer -------
async function downloadDriveFile(drive, fileId) {
  const res = await drive.files.get(
    { fileId, alt: 'media' },
    { responseType: 'arraybuffer' }
  );
  return Buffer.from(res.data);
}

// ------- Upload to Supabase Storage -------
async function uploadToStorage(buf, storagePath, contentType) {
  // mirror path exactly in the bucket
  const { error } = await supabase
    .storage
    .from(SUPABASE_BUCKET)
    .upload(storagePath, buf, {
      contentType: contentType || 'application/octet-stream',
      upsert: true,
    });

  if (error) throw error;
  return storagePath; // we mirror 1:1
}

// --- tiny label helpers (best-effort, safe to keep simple) ---
function categoryFromPath(p) {
  // e.g., "Powersports/Kawasaki/..." -> "Powersports"
  return p.split('/')[0] || null;
}
function brandFromPath(p) {
  // e.g., "Powersports/Kawasaki/..." -> "Kawasaki"
  const segs = p.split('/');
  return segs.length > 1 ? segs[1] : null;
}
function titleFromName(name) {
  return name.replace(/\.pdf$/i, '').trim();
}

// ------- Upsert one manual row -------
async function upsertManualRow(row) {
  const { error } = await supabase.from('manuals').upsert(row);
  if (error) throw error;
}

// ------- Main -------
async function main() {
  console.log('SYNC BUILD tag: debug-logs-v1');
  console.log('Env OK. Starting sync…');

  const auth = parseServiceAccount();
  const drive = google.drive({ version: 'v3', auth });

  console.log('Listing Drive files under root:', '***');
  const files = await listDrivePdfs(drive, GOOGLE_DRIVE_FOLDER_ID, '');
  console.log(`Found ${files.length} files (including non-PDF).`);

  let uploaded = 0;
  let skipped = 0;
  let upserted = 0;

  for (const f of files) {
    try {
      // Size gate (skip early)
      if (f.size > MAX_UPLOAD_BYTES) {
        console.warn('SKIP (too large):', f.name, `${Math.round(f.size / 1024 / 1024)}MB`);
        skipped++;
        continue;
      }

      // Download
      const buf = await downloadDriveFile(drive, f.id);

      // If Drive didn't report size, double-check with actual buffer length
      if (buf.length > MAX_UPLOAD_BYTES) {
        console.warn('SKIP (too large after download):', f.name, `${Math.round(buf.length / 1024 / 1024)}MB`);
        skipped++;
        continue;
      }

      // Derive metadata
      const sum = md5(buf);
      const ext = mime.extension(f.mimeType) || f.name.split('.').pop() || '';
      const storagePath = f.path; // mirror Drive path
      const category = categoryFromPath(f.path);
      const brand = brandFromPath(f.path);
      const title = titleFromName(f.name);

      // Upload to Storage
      try {
        console.log('STEP storage.upload ->', storagePath, f.mimeType);
        await uploadToStorage(buf, storagePath, f.mimeType || mime.lookup(ext) || 'application/pdf');
        console.log('OK storage.upload <-', storagePath);
        uploaded++;
      } catch (e) {
        console.error('STORAGE_RLS_OR_UPLOAD_ERROR:', e.message || e);
        throw e;
      }

      // Upsert into manuals table
      try {
        console.log('STEP db.upsert -> ***');
        const row = {
          source_provider: 'google_drive',
          drive_file_id: f.id,
          drive_path: f.path,
          title,
          brand,
          category,
          mime_type: f.mimeType || 'application/pdf',
          checksum: sum,
          storage_path: storagePath,
          inserted_at: now(),
          updated_at: now(),
        };
        await upsertManualRow(row);
        console.log('OK db.upsert <-', f.id);
        upserted++;
      } catch (e) {
        console.error('DB_RLS_OR_UPSERT_ERROR:', e.message || e);
        throw e;
      }

    } catch (err) {
      // keep going on next file
      console.error('UNCAUGHT ERROR (file-level):', err.message || err);
    }
  }

  console.log('--- SUMMARY ---');
  console.log('Uploaded:', uploaded);
  console.log('Skipped (too large):', skipped);
  console.log('Upserted rows:', upserted);
}

// Run
main().catch((e) => {
  console.error('UNCAUGHT ERROR (top-level):', e.message || e);
  process.exit(1);
});

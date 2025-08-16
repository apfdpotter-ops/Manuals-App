// pages/api/data.ts
import type { NextApiRequest, NextApiResponse } from 'next';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const DATA_URL = process.env.DATA_URL;
  if (!DATA_URL) return res.status(500).json({ error: 'DATA_URL not set' });

  try {
    const r = await fetch(DATA_URL, { cache: 'no-store' });
    if (!r.ok) return res.status(r.status).json({ error: `Upstream ${r.status}` });
    const json = await r.json();
    res.status(200).json(json);
  } catch (e: any) {
    res.status(500).json({ error: e.message || String(e) });
  }
}

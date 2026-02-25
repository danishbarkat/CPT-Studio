import express from 'express';
import multer from 'multer';
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import { parse } from 'csv-parse/sync';
import XLSX from 'xlsx';
import serverless from 'serverless-http';
import url from 'url';

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
const upload = multer({ dest: path.join(__dirname, 'uploads') });

// In-memory storage for sources
const sources = new Map();

function cleanNumber(val) {
  if (val === undefined || val === null || val === '') return null;
  const num = Number(val);
  return Number.isFinite(num) ? num : null;
}

function normalizeRow({ code, description, rate, billing_class, negotiated_type, expiration_date }) {
  const safeCode = String(code || '').trim();
  if (!safeCode) return null;
  return {
    code: safeCode,
    description: description || '',
    rates: [
      {
        negotiated_rate: cleanNumber(rate),
        billing_class: billing_class || null,
        negotiated_type: negotiated_type || null,
        expiration_date: expiration_date || null
      }
    ]
  };
}

function ingestCSV(buffer) {
  const text = buffer.toString('utf8');
  const records = parse(text, { columns: true, skip_empty_lines: true });
  const out = {};
  for (const row of records) {
    const code = row.code || row.cpt || row.cpt_code;
    const rate = row.negotiated_rate || row.rate || row.amount || row.price;
    const descr = row.description || row.desc || '';
    const billingClass = row.billing_class || row.class || null;
    const negotiatedType = row.negotiated_type || null;
    const expiry = row.expiration_date || row.expiry || null;
    const norm = normalizeRow({ code, description: descr, rate, billing_class: billingClass, negotiated_type: negotiatedType, expiration_date: expiry });
    if (norm) out[norm.code] = norm;
  }
  return out;
}

function ingestXLSX(buffer) {
  const wb = XLSX.read(buffer, { type: 'buffer' });
  const sheet = wb.SheetNames[0];
  const json = XLSX.utils.sheet_to_json(wb.Sheets[sheet]);
  return ingestCSV(Buffer.from(XLSX.utils.sheet_to_csv(wb.Sheets[sheet])));
}

function ingestJSON(obj) {
  // If already map of codes
  if (obj && typeof obj === 'object' && !Array.isArray(obj) && Object.values(obj)[0]?.rates) {
    return obj;
  }
  // Try payer in-network structure
  if (obj && obj.in_network_files) {
    // Not supporting index fetch in Node version; return empty
    return {};
  }
  return {};
}

function storeSource(name, cptData, type = 'csv') {
  sources.set(name, { source_name: name, type, cpt_data: cptData });
  return {
    success: true,
    source_name: name,
    type,
    cpt_count: Object.keys(cptData).length,
    cpt_data: cptData
  };
}

async function fetchUrlContent(urlStr) {
  const resp = await axios.get(urlStr, { responseType: 'arraybuffer' });
  return Buffer.from(resp.data);
}

// Comparison logic (simplified)
function compareSources(s1, s2, compare_rule = 'max') {
  const a = sources.get(s1);
  const b = sources.get(s2);
  if (!a || !b) return { success: false, message: 'Sources not loaded' };
  const data1 = a.cpt_data || {};
  const data2 = b.cpt_data || {};
  const codes = new Set([...Object.keys(data1), ...Object.keys(data2)]);
  const higher_in_source1 = [];
  const higher_in_source2 = [];
  const equal = [];
  let totalHigher1 = 0;
  let totalHigher2 = 0;

  const pickRate = (item) => {
    if (!item || !item.rates || !item.rates.length) return 0;
    const vals = item.rates.map(r => Number(r.negotiated_rate || 0)).filter(Number.isFinite);
    if (!vals.length) return 0;
    switch (compare_rule) {
      case 'min': return Math.min(...vals);
      case 'avg': return vals.reduce((a, c) => a + c, 0) / vals.length;
      default: return Math.max(...vals);
    }
  };

  codes.forEach(code => {
    const item1 = data1[code];
    const item2 = data2[code];
    const rate1 = pickRate(item1);
    const rate2 = pickRate(item2);
    if (!item1) {
      higher_in_source2.push({ code, source2_rate: rate2, source1_rate: 0, difference: rate1 - rate2, percent_difference: -100, source2_description: item2?.description || '' });
      totalHigher2 += rate2;
      return;
    }
    if (!item2) {
      higher_in_source1.push({ code, source1_rate: rate1, source2_rate: 0, difference: rate1 - rate2, percent_difference: 100, source1_description: item1?.description || '' });
      totalHigher1 += rate1;
      return;
    }
    if (Math.abs(rate1 - rate2) < 1e-6) {
      equal.push({ code, source1_rate: rate1, source2_rate: rate2, difference: 0, percent_difference: 0, descriptions_match: true, source1_description: item1.description, source2_description: item2.description });
    } else if (rate1 > rate2) {
      higher_in_source1.push({ code, source1_rate: rate1, source2_rate: rate2, difference: rate1 - rate2, percent_difference: ((rate1 - rate2) / rate2) * 100, source1_description: item1.description, source2_description: item2.description });
      totalHigher1 += rate1 - rate2;
    } else {
      higher_in_source2.push({ code, source1_rate: rate1, source2_rate: rate2, difference: rate1 - rate2, percent_difference: ((rate2 - rate1) / rate1) * 100, source1_description: item1.description, source2_description: item2.description });
      totalHigher2 += rate2 - rate1;
    }
  });

  return {
    success: true,
    comparison: {
      source1: s1,
      source2: s2,
      total_compared: codes.size,
      higher_in_source1,
      higher_in_source2,
      equal,
      total_higher_in_source1_amount: totalHigher1,
      total_higher_in_source2_amount: totalHigher2,
      higher_in_source1_count: higher_in_source1.length,
      higher_in_source2_count: higher_in_source2.length,
      equal_count: equal.length
    }
  };
}

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use('/static', express.static(path.join(__dirname, 'templates')));

// Serve UI
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'templates', 'index.html'));
});

// Upload endpoint
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    const sourceName = req.body.source_name || 'Source';
    let buffer;
    if (req.file) {
      buffer = fs.readFileSync(req.file.path);
      fs.unlink(req.file.path, () => {});
    } else if (req.body.url) {
      buffer = await fetchUrlContent(req.body.url);
    } else {
      return res.json({ success: false, message: 'No file or URL provided.' });
    }

    let cptData = {};
    let type = 'csv';
    if (req.file?.originalname?.endsWith('.xlsx') || req.file?.originalname?.endsWith('.xls')) {
      cptData = ingestXLSX(buffer);
      type = 'excel';
    } else if (req.file?.originalname?.endsWith('.json') || req.body.url?.endsWith('.json')) {
      const obj = JSON.parse(buffer.toString('utf8'));
      cptData = ingestJSON(obj);
      type = 'direct_in_network_json';
    } else {
      cptData = ingestCSV(buffer);
      type = 'csv';
    }

    return res.json(storeSource(sourceName, cptData, type));
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, message: 'Upload failed', error: err.message });
  }
});

// Test data loader
app.post('/load_test_data', (req, res) => {
  const fp = path.join(__dirname, 'test_pricing_data.json');
  const data = JSON.parse(fs.readFileSync(fp, 'utf8'));
  sources.set('Test Insurance', { source_name: 'Test Insurance', type: 'json', cpt_data: data });
  res.json({ success: true, source_name: 'Test Insurance', cpt_count: Object.keys(data).length, cpt_data: data });
});

// Compare
app.post('/compare', (req, res) => {
  const { source1, source2, compare_rule } = req.body;
  const result = compareSources(source1, source2, compare_rule || 'max');
  res.json(result);
});

// Export comparison CSV
app.get('/export_comparison_csv', (req, res) => {
  const { source1, source2, compare_rule } = req.query;
  const result = compareSources(source1, source2, compare_rule || 'max');
  if (!result.success) return res.status(400).json(result);
  const { comparison } = result;
  const rows = [
    ['Bucket', 'CPT Code', 'Source1 Rate', 'Source2 Rate', 'Difference', 'Percent'],
    ...comparison.higher_in_source1.map(r => ['Higher in ' + comparison.source1, r.code, r.source1_rate, r.source2_rate, r.difference, r.percent_difference]),
    ...comparison.higher_in_source2.map(r => ['Higher in ' + comparison.source2, r.code, r.source1_rate, r.source2_rate, r.difference, r.percent_difference]),
    ...comparison.equal.map(r => ['Equal', r.code, r.source1_rate, r.source2_rate, 0, 0])
  ];
  const csv = rows.map(r => r.join(',')).join('\n');
  res.setHeader('Content-Disposition', `attachment; filename="${comparison.source1}_vs_${comparison.source2}.csv"`);
  res.setHeader('Content-Type', 'text/csv');
  res.send(csv);
});

// Export single source CSV
app.get('/export_source_csv', (req, res) => {
  const name = req.query.source;
  const src = sources.get(name);
  if (!src) return res.status(404).json({ success: false, message: 'Source not found' });
  const rows = [['code', 'description', 'negotiated_rate', 'billing_class']];
  Object.values(src.cpt_data || {}).forEach(item => {
    const rate = item.rates?.[0] || {};
    rows.push([item.code, item.description, rate.negotiated_rate ?? '', rate.billing_class ?? '']);
  });
  const csv = rows.map(r => r.join(',')).join('\n');
  res.setHeader('Content-Disposition', `attachment; filename="${name}_cpt_pricing.csv"`);
  res.setHeader('Content-Type', 'text/csv');
  res.send(csv);
});

// Not implemented endpoints from Python version
app.all(['/upload_multipart_part', '/finalize_multipart', '/load_paginated', '/fetch_pricing', '/export_incremental_comparison_csv'], (req, res) => {
  res.status(400).json({ success: false, message: 'This endpoint is not supported in the Node build yet.' });
});

// Local dev server
if (process.env.NODE_ENV !== 'production') {
  const port = process.env.PORT || 5001;
  app.listen(port, () => console.log(`CPT Studio Node server running on http://localhost:${port}`));
}

// Export for Vercel
export const handler = serverless(app);
export default app;

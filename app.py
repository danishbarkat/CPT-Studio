from flask import Flask, render_template, request, jsonify, make_response
import json
import ijson
import gzip
import requests
from io import BytesIO, StringIO
import pandas as pd
import openpyxl
import csv
import os
import hashlib
import uuid
import shutil
import time
import math
import datetime


class MultiPartStream:
    """Stream that stitches multiple file parts together as one read() source."""

    def __init__(self, paths):
        self.paths = paths
        self.handles = []
        self.idx = 0
        self.closed = False

    def _open_next(self):
        while self.idx < len(self.paths):
            handle = open(self.paths[self.idx], 'rb')
            self.handles.append(handle)
            self.idx += 1
            return handle
        return None

    def read(self, size=-1):
        if self.closed:
            return b''
        chunks = []
        remaining = size

        while remaining != 0:
            if not self.handles or self.handles[-1].closed:
                handle = self._open_next()
            else:
                handle = self.handles[-1]

            if handle is None:
                break

            if remaining == -1:
                data = handle.read()
            else:
                data = handle.read(remaining)

            if data:
                chunks.append(data)
                if remaining != -1:
                    remaining -= len(data)
            else:
                handle.close()
                continue

        return b''.join(chunks)

    def readable(self):
        return True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self.closed:
            return
        for h in self.handles:
            try:
                h.close()
            except Exception:
                pass
        self.closed = True

app = Flask(__name__)

class CPTPricingAnalyzer:
    def __init__(self):
        self.data_sources = {}
        self.cpt_pricing = {}  # Store CPT pricing by source
        self.cache_dir = os.path.join(os.path.dirname(__file__), 'cached_mrf_files')
        os.makedirs(self.cache_dir, exist_ok=True)
        self.upload_dir = os.path.join(self.cache_dir, 'uploads')
        os.makedirs(self.upload_dir, exist_ok=True)
        self.comparison_session_dir = os.path.join(self.cache_dir, 'comparison_sessions')
        os.makedirs(self.comparison_session_dir, exist_ok=True)
        self.large_file_threshold = 300 * 1024 * 1024  # 300 MB
        self.preview_limit = 10000
        self.multipart_sessions = {}  # session_id -> {'paths': [...], 'source_name': str}
        self.incremental_compare_sessions = {}  # session_id -> runtime state
        self.incremental_sample_limit = 2000
        self.incremental_only_in_source1_sample_limit = 100
        self.incremental_only_in_source2_sample_limit = 50

    def _to_float(self, value, default=0.0):
        try:
            if value is None:
                return default
            val = float(value)
            if not math.isfinite(val):
                return default
            return val
        except (TypeError, ValueError):
            return default

    def _try_float(self, value):
        """Return float(value) if numeric+finite else None (prevents biasing AVG/MEDIAN with 0)."""
        try:
            if value is None:
                return None
            val = float(value)
            if not math.isfinite(val):
                return None
            return val
        except (TypeError, ValueError):
            return None

    def _parse_date_yyyy_mm_dd(self, value):
        if not value:
            return None
        try:
            return datetime.date.fromisoformat(str(value)[:10])
        except Exception:
            return None

    def _filter_rates(self, rates, negotiated_type=None, exclude_expired=False, as_of=None):
        negotiated_type = (negotiated_type or '').strip().lower()
        as_of = as_of or datetime.date.today()
        out = []

        for rate in rates or []:
            if negotiated_type:
                nt = (rate.get('negotiated_type') or '').strip().lower()
                if nt != negotiated_type:
                    continue

            if exclude_expired:
                exp = self._parse_date_yyyy_mm_dd(rate.get('expiration_date'))
                if exp is not None and exp < as_of:
                    continue

            out.append(rate)

        return out

    class _P2Quantile:
        """
        P² (P-square) streaming quantile estimator.
        Constant memory; good for large streams where exact median is expensive.
        """

        def __init__(self, quantile=0.5):
            self.q = float(quantile)
            self.n = 0
            self.initial = []
            # Marker positions (n_i), desired positions (n'_i), increments (d_i), heights (q_i)
            self.ni = [0, 0, 0, 0, 0]
            self.np = [0.0, 0.0, 0.0, 0.0, 0.0]
            self.di = [0.0, 0.0, 0.0, 0.0, 0.0]
            self.qi = [0.0, 0.0, 0.0, 0.0, 0.0]

        def add(self, x):
            x = float(x)
            self.n += 1

            # Bootstrap with first 5 samples
            if self.n <= 5:
                self.initial.append(x)
                if self.n == 5:
                    self.initial.sort()
                    self.qi = self.initial[:]  # marker heights
                    self.ni = [1, 2, 3, 4, 5]   # marker positions
                    q = self.q
                    self.np = [1.0, 1.0 + 2.0 * q, 1.0 + 4.0 * q, 3.0 + 2.0 * q, 5.0]
                    self.di = [0.0, q / 2.0, q, (1.0 + q) / 2.0, 1.0]
                return

            # Find k: bucket for x and update end markers if needed
            if x < self.qi[0]:
                self.qi[0] = x
                k = 0
            elif x < self.qi[1]:
                k = 0
            elif x < self.qi[2]:
                k = 1
            elif x < self.qi[3]:
                k = 2
            elif x < self.qi[4]:
                k = 3
            else:
                self.qi[4] = x
                k = 3

            # Increment positions of markers above k
            for i in range(k + 1, 5):
                self.ni[i] += 1

            # Update desired positions
            for i in range(5):
                self.np[i] += self.di[i]

            # Adjust heights of markers 2..4 (index 1..3)
            for i in (1, 2, 3):
                d = self.np[i] - self.ni[i]
                if (d >= 1.0 and self.ni[i + 1] - self.ni[i] > 1) or (d <= -1.0 and self.ni[i - 1] - self.ni[i] < -1):
                    s = 1 if d > 0 else -1
                    # Parabolic prediction
                    qip1 = self.qi[i + 1]
                    qi = self.qi[i]
                    qim1 = self.qi[i - 1]
                    nip1 = self.ni[i + 1]
                    ni = self.ni[i]
                    nim1 = self.ni[i - 1]

                    denom = (nip1 - nim1)
                    if denom == 0:
                        continue

                    qp = qi + (s / denom) * (
                        (ni - nim1 + s) * (qip1 - qi) / (nip1 - ni) +
                        (nip1 - ni - s) * (qi - qim1) / (ni - nim1)
                    )

                    # If parabolic is out of bounds, use linear
                    if qp <= min(qim1, qip1) or qp >= max(qim1, qip1) or math.isnan(qp):
                        qp = qi + s * (self.qi[i + s] - qi) / (self.ni[i + s] - ni)

                    self.qi[i] = qp
                    self.ni[i] += s

        def value(self):
            if self.n == 0:
                return 0.0
            if self.n <= 5:
                vals = sorted(self.initial)
                mid = len(vals) // 2
                if len(vals) % 2 == 1:
                    return float(vals[mid])
                return float((vals[mid - 1] + vals[mid]) / 2.0)
            # Median marker is index 2
            return float(self.qi[2])

    def _rates_summary(self, rates):
        total = 0.0
        count = 0
        min_rate = None
        max_rate = None

        for rate in rates or []:
            val = self._try_float(rate.get('negotiated_rate'))
            if val is None:
                continue
            total += val
            count += 1
            min_rate = val if min_rate is None else min(min_rate, val)
            max_rate = val if max_rate is None else max(max_rate, val)

        avg_rate = (total / count) if count else 0.0
        return {
            'count': count,
            'avg': avg_rate,
            'min': min_rate if min_rate is not None else 0.0,
            'max': max_rate if max_rate is not None else 0.0,
        }

    def _rates_summary_by_class(self, rates):
        classes = {}

        for rate in rates or []:
            billing_class = (rate.get('billing_class') or 'unknown').strip() or 'unknown'
            if billing_class not in classes:
                classes[billing_class] = {'sum': 0.0, 'count': 0, 'min': None, 'max': None}
            self._update_running_summary(classes[billing_class], rate.get('negotiated_rate'))

        # Compute averages and pick a representative class (prefer non-unknown)
        rep_class = None
        rep_avg = 0.0

        for cls_name, summary in classes.items():
            avg = (summary['sum'] / summary['count']) if summary['count'] else 0.0
            summary['avg'] = avg
            summary['min'] = summary['min'] if summary['min'] is not None else 0.0
            summary['max'] = summary['max'] if summary['max'] is not None else 0.0

        # Prefer known classes if present
        ordered = [c for c in classes.keys() if c != 'unknown'] + (['unknown'] if 'unknown' in classes else [])
        for cls_name in ordered:
            avg = classes[cls_name].get('avg', 0.0)
            if rep_class is None or avg > rep_avg:
                rep_class = cls_name
                rep_avg = avg

        return {
            'classes': classes,
            'representative_class': rep_class or 'unknown',
            'representative_avg': rep_avg,
        }

    def _max_rate_with_class(self, rates):
        max_rate = 0.0
        max_class = 'unknown'
        count = 0

        for rate in rates or []:
            val = self._try_float(rate.get('negotiated_rate'))
            if val is None:
                continue
            count += 1
            if val > max_rate:
                max_rate = val
                max_class = (rate.get('billing_class') or 'unknown').strip() or 'unknown'

        return {'max': max_rate, 'billing_class': max_class, 'count': count}

    def _max_rate_by_class(self, rates):
        """Return billing_class -> max negotiated_rate for that class."""
        max_by_class = {}
        count = 0
        for rate in rates or []:
            val = self._try_float(rate.get('negotiated_rate'))
            if val is None:
                continue
            count += 1
            billing_class = (rate.get('billing_class') or 'unknown').strip() or 'unknown'
            prev = max_by_class.get(billing_class)
            if prev is None or val > prev:
                max_by_class[billing_class] = val
        return max_by_class, count

    def _context_key(self, rate):
        billing_class = (rate.get('billing_class') or 'unknown').strip() or 'unknown'
        modifiers = rate.get('billing_code_modifier') or []
        if isinstance(modifiers, (list, tuple, set)):
            modifier_key = tuple(sorted(str(m).strip() for m in modifiers if str(m).strip()))
        else:
            modifier_key = (str(modifiers).strip(),) if str(modifiers).strip() else ()
        return billing_class, modifier_key

    def _max_rate_by_context(self, rates):
        """Return (billing_class, modifiers_tuple) -> max negotiated_rate."""
        out = {}
        count = 0
        for rate in rates or []:
            val = self._try_float(rate.get('negotiated_rate'))
            if val is None:
                continue
            count += 1
            key = self._context_key(rate)
            prev = out.get(key)
            if prev is None or val > prev:
                out[key] = val
        return out, count

    def _min_rate_with_class(self, rates):
        min_rate = None
        min_class = 'unknown'
        count = 0

        for rate in rates or []:
            val = self._try_float(rate.get('negotiated_rate'))
            if val is None:
                continue
            count += 1
            if min_rate is None or val < min_rate:
                min_rate = val
                min_class = (rate.get('billing_class') or 'unknown').strip() or 'unknown'

        return {'min': (min_rate if min_rate is not None else 0.0), 'billing_class': min_class, 'count': count}

    def _median_rate(self, rates):
        values = []
        for r in (rates or []):
            val = self._try_float(r.get('negotiated_rate'))
            if val is not None:
                values.append(val)
        if not values:
            return 0.0
        values.sort()
        mid = len(values) // 2
        if len(values) % 2 == 1:
            return values[mid]
        return (values[mid - 1] + values[mid]) / 2.0

    def _rate_for_rule(self, rates, rule, negotiated_type=None, exclude_expired=False, as_of=None):
        """
        Returns: (value: float, billing_class: str, meta: dict)
        rule one of:
          - max
          - min
          - avg
          - median
          - max_avg_by_billing_class
        """
        rule = (rule or 'max').strip().lower()
        rates = self._filter_rates(rates, negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)

        if rule == 'max':
            info = self._max_rate_with_class(rates)
            return info['max'], info.get('billing_class', 'unknown'), {'count': info.get('count', 0)}

        if rule == 'min':
            info = self._min_rate_with_class(rates)
            return info['min'], info.get('billing_class', 'unknown'), {'count': info.get('count', 0)}

        if rule == 'avg':
            summary = self._rates_summary(rates)
            return summary['avg'], 'unknown', {'count': summary.get('count', 0)}

        if rule == 'median':
            numeric_count = 0
            for r in (rates or []):
                if self._try_float(r.get('negotiated_rate')) is not None:
                    numeric_count += 1
            return self._median_rate(rates), 'unknown', {'count': numeric_count}

        if rule == 'max_avg_by_billing_class':
            by_class = self._rates_summary_by_class(rates)
            return by_class.get('representative_avg', 0.0), by_class.get('representative_class', 'unknown'), {
                'classes': by_class.get('classes', {})
            }

        if rule == 'all_classes':
            raise ValueError("compare_rule=all_classes returns multiple values; handle it in compare_pricing/incremental mode.")

        if rule == 'per_occurrence':
            raise ValueError("compare_rule=per_occurrence is occurrence-based; handle it in compare_pricing/incremental mode.")

        if rule == 'context':
            raise ValueError("compare_rule=context returns multiple values; handle it in compare_pricing/incremental mode.")

        # fallback
        info = self._max_rate_with_class(rates)
        return info['max'], info.get('billing_class', 'unknown'), {'count': info.get('count', 0), 'fallback_rule': 'max'}

    def _update_running_summary(self, summary, value):
        value = self._try_float(value)
        if value is None:
            return
        summary['sum'] += value
        summary['count'] += 1
        summary['min'] = value if summary['min'] is None else min(summary['min'], value)
        summary['max'] = value if summary['max'] is None else max(summary['max'], value)
        
    def load_json_file(self, file_path_or_url, source_name):
        """Load JSON from file or URL"""
        try:
            if file_path_or_url.startswith('http'):
                response = requests.get(file_path_or_url, timeout=30)
                data = json.loads(response.text)
            else:
                with open(file_path_or_url, 'r') as f:
                    data = json.load(f)
            
            self.data_sources[source_name] = data
            return True, "Data loaded successfully"
        except Exception as e:
            return False, f"Error loading data: {str(e)}"
    
    def load_excel_file(self, file_path, source_name):
        """Load CPT pricing from Excel file"""
        try:
            xl = pd.ExcelFile(file_path)
            
            found_data = False
            cpt_data = {}
            
            for sheet_name in xl.sheet_names:
                try:
                    df = pd.read_excel(file_path, sheet_name=sheet_name)
                    cpt_col = None
                    price_col = None
                    desc_col = None
                    
                    for col in df.columns:
                        col_lower = str(col).lower().strip()
                        if any(x in col_lower for x in ['cpt', 'code', 'proc_cd', 'procedure', 'hcpcs']) and 'desc' not in col_lower:
                            cpt_col = col
                        elif any(x in col_lower for x in ['price', 'rate', 'amount', 'cost', 'fee', 'allowance', 'calc_rate']):
                            price_col = col
                        elif any(x in col_lower for x in ['desc', 'description', 'name']):
                            desc_col = col
                    
                    if cpt_col and price_col:
                        found_data = True
                        for _, row in df.iterrows():
                            cpt_code = str(row[cpt_col]).strip()
                            price = row[price_col]
                            description = row[desc_col] if desc_col else "No description"
                            
                            if pd.isna(cpt_code) or cpt_code == '' or cpt_code == 'nan':
                                continue
                            
                            try:
                                price = float(price)
                            except:
                                price = 0.0
                            
                            cpt_data[cpt_code] = {
                                'description': str(description),
                                'rates': [{
                                    'billing_class': 'excel_import',
                                    'negotiated_rate': price,
                                    'billing_code_modifier': [],
                                    'service_code': [],
                                    'negotiated_type': 'excel_import',
                                    'expiration_date': None
                                }]
                            }
                        break
                        
                except Exception:
                    continue
            
            if not found_data:
                return False, "Could not identify CPT code and price columns in any sheet. Please ensure your Excel has columns like 'CPT', 'Code', 'Proc_CD' and 'Price', 'Rate', 'Fee'."
            
            self.cpt_pricing[source_name] = cpt_data
            return True, f"Loaded {len(cpt_data)} CPT codes from Excel."
            
        except Exception as e:
            return False, f"Error loading Excel: {str(e)}"

    def load_csv_file(self, file_path, source_name):
        """Load CPT pricing from CSV without loading entire file into memory"""
        try:
            with open(file_path, 'r', encoding='utf-8-sig', newline='') as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader, None)
                if not headers:
                    return False, "CSV file is missing a header row."

                cpt_index = None
                price_index = None
                desc_index = None

                for idx, header in enumerate(headers):
                    col_lower = str(header).lower().strip()
                    if cpt_index is None and 'desc' not in col_lower and any(x in col_lower for x in ['cpt', 'code', 'proc_cd', 'procedure', 'hcpcs']):
                        cpt_index = idx
                    elif price_index is None and any(x in col_lower for x in ['price', 'rate', 'amount', 'cost', 'fee', 'allowance', 'calc_rate']):
                        price_index = idx
                    elif desc_index is None and any(x in col_lower for x in ['desc', 'description', 'name']):
                        desc_index = idx

                if cpt_index is None or price_index is None:
                    return False, "Could not find CPT code and price columns in CSV. Please ensure headers include CPT/Code and Price/Rate."

                cpt_data = {}
                for row in reader:
                    if len(row) <= max(cpt_index, price_index):
                        continue
                    cpt_code = str(row[cpt_index]).strip()
                    if not cpt_code:
                        continue
                    price_val = row[price_index]
                    try:
                        price = float(price_val)
                    except (TypeError, ValueError):
                        price = 0.0
                    description = str(row[desc_index]).strip() if desc_index is not None and len(row) > desc_index else 'No description'

                    cpt_data[cpt_code] = {
                        'description': description,
                        'rates': [{
                            'billing_class': 'csv_import',
                            'negotiated_rate': price,
                            'billing_code_modifier': [],
                            'service_code': [],
                            'negotiated_type': 'csv_import',
                            'expiration_date': None
                        }]
                    }

            self.cpt_pricing[source_name] = cpt_data
            return True, f"Loaded {len(cpt_data)} CPT codes from CSV.", cpt_data
        except Exception as e:
            return False, f"Error loading CSV: {str(e)}", {}

    def save_uploaded_file(self, file_storage, prefix='upload'):
        """Persist uploaded files to disk for streaming-friendly processing"""
        ext = os.path.splitext(file_storage.filename or 'upload')[1]
        safe_name = f"{prefix}_{uuid.uuid4().hex}{ext}"
        dest_path = os.path.join(self.upload_dir, safe_name)
        file_storage.stream.seek(0)
        with open(dest_path, 'wb') as dest:
            shutil.copyfileobj(file_storage.stream, dest)
        file_storage.stream.seek(0)
        return dest_path

    def add_multipart_part(self, session_id, file_storage, source_name):
        """Store a single part in a multi-part session"""
        if not session_id:
            session_id = uuid.uuid4().hex
        if session_id not in self.multipart_sessions:
            self.multipart_sessions[session_id] = {
                'paths': [],
                'source_name': source_name or f'Source_{session_id[:6]}',
                'filenames': set()
            }

        original_name = os.path.basename(file_storage.filename or '').strip()
        if original_name and original_name in self.multipart_sessions[session_id]['filenames']:
            # Ignore duplicates to prevent double-counting during incremental uploads
            return session_id, None, len(self.multipart_sessions[session_id]['paths']), True, original_name

        part_path = self.save_uploaded_file(file_storage, f'part_{session_id}')
        self.multipart_sessions[session_id]['paths'].append(part_path)
        if original_name:
            self.multipart_sessions[session_id]['filenames'].add(original_name)
        return session_id, part_path, len(self.multipart_sessions[session_id]['paths']), False, original_name

    def get_multipart_paths(self, session_id):
        session = self.multipart_sessions.get(session_id)
        if not session:
            return []
        return session.get('paths', [])

    def _incremental_state_to_payload(self, state):
        comparison = state['comparison'].copy()

        comparison['higher_in_source1'] = list(comparison.get('higher_in_source1', []))
        comparison['higher_in_source2'] = list(comparison.get('higher_in_source2', []))
        comparison['equal'] = list(comparison.get('equal', []))
        comparison['only_in_source1_sample'] = list(comparison.get('only_in_source1_sample', []))
        comparison['only_in_source2_sample'] = list(comparison.get('only_in_source2_sample', []))

        # Keep payload compact, but counts accurate.
        comparison['higher_in_source1_count'] = int(comparison.get('higher_in_source1_count', len(comparison['higher_in_source1'])))
        comparison['higher_in_source2_count'] = int(comparison.get('higher_in_source2_count', len(comparison['higher_in_source2'])))
        comparison['equal_count'] = int(comparison.get('equal_count', len(comparison['equal'])))

        comparison['parts_processed'] = int(state.get('parts_processed', 0))
        comparison['last_part'] = state.get('last_part')
        comparison['updated_at'] = state.get('updated_at')
        comparison['session_id'] = state.get('session_id')
        comparison['incremental'] = True

        return comparison

    def _persist_incremental_session_summary(self, state):
        safe_session_id = state.get('session_id') or uuid.uuid4().hex
        path = os.path.join(self.comparison_session_dir, f'{safe_session_id}.json')

        payload = self._incremental_state_to_payload(state)
        # Remove potentially large arrays if someone cranks limits.
        payload['meta'] = {
            'note': 'This is a saved summary + samples. Full per-code results are not stored.',
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)

        return path

    def _get_or_create_incremental_session(self, session_id, source1_name, baseline_source_name):
        if not session_id:
            session_id = uuid.uuid4().hex

        existing = self.incremental_compare_sessions.get(session_id)
        if existing:
            if existing.get('baseline_source') != baseline_source_name:
                raise ValueError('baseline_source cannot change for an existing session_id.')

            # Update friendly name if provided
            if source1_name:
                existing['comparison']['source1'] = source1_name
            return session_id, existing

        baseline_data = self.cpt_pricing.get(baseline_source_name) or {}
        state = {
            'session_id': session_id,
            'baseline_source': baseline_source_name,
            'baseline_key_map': None,
            'parts_processed': 0,
            'last_part': None,
            'updated_at': None,
            'seen_source1_codes': set(),
            'matched_baseline_codes': set(),
            'only_in_source1_codes': set(),
            'baseline_rate_cache': {},
            'source1_rate_summary': {},  # code -> per-rule streaming summary
            'code_bucket': {},  # code -> 'higher_in_source1'|'higher_in_source2'|'equal'
            'code_diff_cache': {},  # code -> (source1_avg - source2_avg)
            'code_class_bucket': {},  # f"{code}|{billing_class}" -> bucket (all_classes)
            'code_class_diff_cache': {},  # f"{code}|{billing_class}" -> diff
            'matched_code_classes': set(),  # set of f"{code}|{billing_class}"
            'occurrence_counter': 0,
            'sample_by_bucket': {
                'higher_in_source1': {},
                'higher_in_source2': {},
                'equal': {}
            },
            'comparison': {
                'source1': source1_name or 'Source 1 (parts)',
                'source2': baseline_source_name,
                'higher_in_source1': [],
                'higher_in_source2': [],
                'equal': [],
                'only_in_source1_sample': [],
                'only_in_source2_sample': [],
                'only_in_source1_count': 0,
                'only_in_source2_count': len(baseline_data),
                'total_compared': 0,
                'total_source1_count': 0,
                'total_source2': len(baseline_data),
                'higher_in_source1_count': 0,
                'higher_in_source2_count': 0,
                'equal_count': 0,
                'total_higher_in_source1_amount': 0,
                'total_higher_in_source2_amount': 0
            }
        }
        self.incremental_compare_sessions[session_id] = state
        return session_id, state

    def _init_source1_summary(self, description, rule):
        rule = (rule or 'max').strip().lower()
        if rule == 'max':
            return {'description': description, 'max': 0.0, 'billing_class': 'unknown', 'count': 0}
        if rule == 'min':
            return {'description': description, 'min': None, 'billing_class': 'unknown', 'count': 0}
        if rule == 'avg':
            return {'description': description, 'sum': 0.0, 'count': 0}
        if rule == 'median':
            return {'description': description, 'p2': self._P2Quantile(0.5), 'count': 0}
        if rule == 'all_classes':
            return {'description': description, 'classes': {}}
        if rule == 'max_avg_by_billing_class':
            return {'description': description, 'classes': {}}
        if rule == 'per_occurrence':
            return {'description': description}
        raise ValueError('Unsupported compare_rule for streaming mode.')

    def _update_source1_summary(self, summary, price, rule):
        rule = (rule or 'max').strip().lower()
        rate_val = self._try_float(price.get('negotiated_rate', 0))
        billing_class = (price.get('billing_class') or 'unknown').strip() or 'unknown'

        if rule == 'max':
            if rate_val is not None:
                summary['count'] += 1
                if rate_val > summary.get('max', 0.0):
                    summary['max'] = rate_val
                    summary['billing_class'] = billing_class
            return

        if rule == 'min':
            if rate_val is not None:
                summary['count'] += 1
                if summary.get('min') is None or rate_val < summary.get('min'):
                    summary['min'] = rate_val
                    summary['billing_class'] = billing_class
            return

        if rule == 'avg':
            if rate_val is not None:
                summary['sum'] += rate_val
                summary['count'] += 1
            return

        if rule == 'median':
            if rate_val is not None:
                summary['count'] += 1
                p2 = summary.get('p2')
                if not p2:
                    p2 = self._P2Quantile(0.5)
                    summary['p2'] = p2
                p2.add(rate_val)
            return

        if rule == 'max_avg_by_billing_class':
            classes = summary.setdefault('classes', {})
            if billing_class not in classes:
                classes[billing_class] = {'sum': 0.0, 'count': 0, 'min': None, 'max': None}
            if rate_val is not None:
                self._update_running_summary(classes[billing_class], rate_val)
            return

        if rule == 'all_classes':
            classes = summary.setdefault('classes', {})
            entry = classes.get(billing_class)
            if entry is None:
                if rate_val is not None:
                    classes[billing_class] = {'max': rate_val, 'count': 1}
            else:
                if rate_val is not None:
                    entry['count'] += 1
                    if rate_val > entry.get('max', 0.0):
                        entry['max'] = rate_val
            return

        raise ValueError('Unsupported compare_rule for streaming mode.')

    def _finalize_source1_value(self, summary, rule):
        rule = (rule or 'max').strip().lower()
        if rule == 'max':
            return summary.get('max', 0.0), summary.get('billing_class', 'unknown'), {'count': summary.get('count', 0)}
        if rule == 'min':
            return (summary.get('min') if summary.get('min') is not None else 0.0), summary.get('billing_class', 'unknown'), {'count': summary.get('count', 0)}
        if rule == 'avg':
            count = summary.get('count', 0)
            return ((summary.get('sum', 0.0) / count) if count else 0.0), 'unknown', {'count': count}
        if rule == 'median':
            p2 = summary.get('p2')
            return (p2.value() if p2 else 0.0), 'unknown', {'count': summary.get('count', 0)}
        if rule == 'all_classes':
            raise ValueError("compare_rule=all_classes returns multiple values; use class-wise comparison.")
        if rule == 'max_avg_by_billing_class':
            by_class = self._rates_summary_by_class([{'negotiated_rate': v['sum'] / v['count'], 'billing_class': cls} for cls, v in (summary.get('classes') or {}).items() if v.get('count')])
            return by_class.get('representative_avg', 0.0), by_class.get('representative_class', 'unknown'), {'classes': by_class.get('classes', {})}
        raise ValueError('Unsupported compare_rule for streaming mode.')

    def incremental_compare_part(self, session_id, part_path, source1_name, baseline_source_name, compare_rule='max', negotiated_type=None, exclude_expired=False, as_of=None):
        """Compare one split JSON part against baseline and accumulate results in-session."""
        if baseline_source_name not in self.cpt_pricing:
            return None, "Baseline source not loaded."

        compare_rule = (compare_rule or 'max').strip().lower()
        negotiated_type = (negotiated_type or '').strip().lower() or None
        exclude_expired = bool(exclude_expired)
        as_of = as_of or datetime.date.today()

        try:
            session_id, state = self._get_or_create_incremental_session(session_id, source1_name, baseline_source_name)
        except ValueError as e:
            return None, str(e)

        comparison = state['comparison']
        baseline_data = self.cpt_pricing[baseline_source_name]
        # Normalize baseline keys to strings once per session (fixes 0 matches in all_classes/per_occurrence)
        if state.get('baseline_key_map') is None:
            state['baseline_key_map'] = {str(k).strip(): v for k, v in baseline_data.items()}
            comparison['total_source2'] = len(state['baseline_key_map'])
        baseline_data = state['baseline_key_map']
        if state.get('compare_rule') and state.get('compare_rule') != compare_rule:
            return None, "compare_rule cannot change for an existing session_id."
        if state.get('negotiated_type') != negotiated_type and state.get('negotiated_type') is not None:
            return None, "negotiated_type cannot change for an existing session_id."
        if state.get('exclude_expired') is not None and bool(state.get('exclude_expired')) != exclude_expired:
            return None, "exclude_expired cannot change for an existing session_id."
        state['compare_rule'] = compare_rule
        comparison['compare_rule'] = compare_rule
        state['negotiated_type'] = negotiated_type
        state['exclude_expired'] = exclude_expired
        comparison['negotiated_type'] = negotiated_type or ''
        comparison['exclude_expired'] = exclude_expired

        # Occurrence-based mode: compare every CPT item occurrence (no de-dupe).
        if compare_rule == 'per_occurrence':
            try:
                with self._open_json_stream(part_path) as stream:
                    parser = ijson.items(stream, 'in_network.item')

                    for item in parser:
                        billing_code = item.get('billing_code')
                        billing_code = str(billing_code).strip() if billing_code is not None else ''
                        if not billing_code or item.get('billing_code_type') != 'CPT':
                            continue

                        description1 = item.get('description', 'No description')

                        if billing_code not in state['seen_source1_codes']:
                            state['seen_source1_codes'].add(billing_code)
                            comparison['total_source1_count'] += 1

                        # Occurrence max within this CPT item
                        occ_rate = 0.0
                        occ_class = 'unknown'
                        if 'negotiated_rates' in item:
                            for rate_info in item['negotiated_rates']:
                                if 'negotiated_prices' in rate_info:
                                    for price in rate_info['negotiated_prices']:
                                        if negotiated_type:
                                            nt = (price.get('negotiated_type') or '').strip().lower()
                                            if nt != negotiated_type:
                                                continue
                                        if exclude_expired:
                                            exp = self._parse_date_yyyy_mm_dd(price.get('expiration_date'))
                                            if exp is not None and exp < as_of:
                                                continue
                                        val = self._to_float(price.get('negotiated_rate', 0))
                                        if val > occ_rate:
                                            occ_rate = val
                                            occ_class = (price.get('billing_class') or 'unknown').strip() or 'unknown'

                        # Aggregate by code: keep only the highest occurrence observed so far
                        s1 = state['source1_rate_summary'].get(billing_code)
                        if not s1:
                            state['source1_rate_summary'][billing_code] = {
                                'description': description1,
                                'max': occ_rate,
                                'billing_class': occ_class
                            }
                        else:
                            if s1.get('description') in (None, '', 'No description') and description1 not in (None, '', 'No description'):
                                s1['description'] = description1
                            if occ_rate > self._to_float(s1.get('max', 0.0)):
                                s1['max'] = occ_rate
                                s1['billing_class'] = occ_class

                        if billing_code not in baseline_data:
                            if billing_code not in state['only_in_source1_codes']:
                                state['only_in_source1_codes'].add(billing_code)
                                comparison['only_in_source1_count'] += 1
                                if len(comparison['only_in_source1_sample']) < self.incremental_only_in_source1_sample_limit:
                                    comparison['only_in_source1_sample'].append({
                                        'code': billing_code,
                                        'billing_class': occ_class,
                                        'description': description1,
                                        'rate': occ_rate
                                    })
                            continue

                        state['matched_baseline_codes'].add(billing_code)
                        comparison['total_compared'] = len(state['matched_baseline_codes'])

                        if billing_code not in state['baseline_rate_cache']:
                            rate2_val, rate2_class, rate2_meta = self._rate_for_rule(
                                baseline_data[billing_code].get('rates', []),
                                'max',
                                negotiated_type=negotiated_type,
                                exclude_expired=exclude_expired,
                                as_of=as_of
                            )
                            state['baseline_rate_cache'][billing_code] = {
                                'value': rate2_val,
                                'billing_class': rate2_class,
                                'meta': rate2_meta
                            }
                        baseline_stats = state['baseline_rate_cache'][billing_code]
                        rate2_val = baseline_stats.get('value', 0.0)
                        description2 = baseline_data[billing_code].get('description', '')

                        s1 = state['source1_rate_summary'][billing_code]
                        rate1_val = self._to_float(s1.get('max', 0.0))

                        diff = abs(rate1_val - rate2_val)
                        percent_diff = (diff / max(rate1_val, rate2_val) * 100) if max(rate1_val, rate2_val) > 0 else 0

                        comp_item = {
                            'code': billing_code,
                            'billing_class': s1.get('billing_class', 'unknown'),
                            'source1_description': s1.get('description', description1),
                            'source2_description': description2,
                            'source1_rate': rate1_val,
                            'source2_rate': rate2_val,
                            'difference': rate1_val - rate2_val,
                            'percent_difference': percent_diff,
                            'rate_basis': 'per_code_highest_occurrence_vs_baseline_max'
                        }

                        prev_bucket = state['code_bucket'].get(billing_code)
                        prev_diff = state['code_diff_cache'].get(billing_code, 0.0)
                        if prev_bucket == 'higher_in_source1':
                            comparison['higher_in_source1_count'] -= 1
                            comparison['total_higher_in_source1_amount'] -= max(prev_diff, 0.0)
                        elif prev_bucket == 'higher_in_source2':
                            comparison['higher_in_source2_count'] -= 1
                            comparison['total_higher_in_source2_amount'] -= max(-prev_diff, 0.0)
                        elif prev_bucket == 'equal':
                            comparison['equal_count'] -= 1

                        if rate1_val > rate2_val:
                            bucket = 'higher_in_source1'
                            comparison['higher_in_source1_count'] += 1
                            comparison['total_higher_in_source1_amount'] += (rate1_val - rate2_val)
                        elif rate2_val > rate1_val:
                            bucket = 'higher_in_source2'
                            comparison['higher_in_source2_count'] += 1
                            comparison['total_higher_in_source2_amount'] += (rate2_val - rate1_val)
                        else:
                            bucket = 'equal'
                            comparison['equal_count'] += 1

                        state['code_bucket'][billing_code] = bucket
                        state['code_diff_cache'][billing_code] = (rate1_val - rate2_val)

                        for bucket_name in ('higher_in_source1', 'higher_in_source2', 'equal'):
                            if billing_code in state['sample_by_bucket'][bucket_name] and bucket_name != bucket:
                                del state['sample_by_bucket'][bucket_name][billing_code]
                        if billing_code in state['sample_by_bucket'][bucket]:
                            state['sample_by_bucket'][bucket][billing_code] = comp_item
                        elif len(state['sample_by_bucket'][bucket]) < self.incremental_sample_limit:
                            state['sample_by_bucket'][bucket][billing_code] = comp_item
            except Exception as e:
                return None, f"Error during incremental comparison: {str(e)}"

            comparison['higher_in_source1'] = list(state['sample_by_bucket']['higher_in_source1'].values())
            comparison['higher_in_source2'] = list(state['sample_by_bucket']['higher_in_source2'].values())
            comparison['equal'] = list(state['sample_by_bucket']['equal'].values())

            comparison['only_in_source2_count'] = max(0, len(baseline_data) - len(state['matched_baseline_codes']))
            comparison['only_in_source2_sample'] = []
            for code, info in baseline_data.items():
                if code in state['matched_baseline_codes']:
                    continue
                rate2_val, _, _ = self._rate_for_rule(
                    info.get('rates', []),
                    'max',
                    negotiated_type=negotiated_type,
                    exclude_expired=exclude_expired,
                    as_of=as_of
                )
                comparison['only_in_source2_sample'].append({
                    'code': code,
                    'description': info.get('description', ''),
                    'rate': rate2_val
                })
                if len(comparison['only_in_source2_sample']) >= self.incremental_only_in_source2_sample_limit:
                    break

            state['parts_processed'] += 1
            state['last_part'] = os.path.basename(part_path)
            state['updated_at'] = int(time.time())
            self._persist_incremental_session_summary(state)

            return self._incremental_state_to_payload(state), "Success"

        try:
            with self._open_json_stream(part_path) as stream:
                parser = ijson.items(stream, 'in_network.item')

                for item in parser:
                    billing_code = item.get('billing_code')
                    billing_code = str(billing_code).strip() if billing_code is not None else ''
                    if not billing_code or item.get('billing_code_type') != 'CPT':
                        continue

                    description1 = item.get('description', 'No description')

                    # Track unique codes (for counts), but aggregate ALL rates for that code.
                    if billing_code not in state['seen_source1_codes']:
                        state['seen_source1_codes'].add(billing_code)
                        comparison['total_source1_count'] += 1

                    if billing_code in baseline_data:
                        if billing_code not in state['source1_rate_summary']:
                            state['source1_rate_summary'][billing_code] = self._init_source1_summary(description1, compare_rule)
                        else:
                            if state['source1_rate_summary'][billing_code].get('description') in (None, '', 'No description') and description1 not in (None, '', 'No description'):
                                state['source1_rate_summary'][billing_code]['description'] = description1

                        if 'negotiated_rates' in item:
                            for rate_info in item['negotiated_rates']:
                                if 'negotiated_prices' in rate_info:
                                    for price in rate_info['negotiated_prices']:
                                        if negotiated_type:
                                            nt = (price.get('negotiated_type') or '').strip().lower()
                                            if nt != negotiated_type:
                                                continue
                                        if exclude_expired:
                                            exp = self._parse_date_yyyy_mm_dd(price.get('expiration_date'))
                                            if exp is not None and exp < as_of:
                                                continue
                                        self._update_source1_summary(state['source1_rate_summary'][billing_code], price, compare_rule)

                        state['matched_baseline_codes'].add(billing_code)
                        comparison['total_compared'] = len(state['matched_baseline_codes'])

                        if billing_code not in state['baseline_rate_cache']:
                            if compare_rule == 'all_classes':
                                max_by_class, count2 = self._max_rate_by_class(baseline_data[billing_code].get('rates', []))
                                state['baseline_rate_cache'][billing_code] = {
                                    'classes': max_by_class,
                                    'meta': {'count': count2}
                                }
                            else:
                                rate2_val, rate2_class, rate2_meta = self._rate_for_rule(
                                    baseline_data[billing_code].get('rates', []),
                                    compare_rule,
                                    negotiated_type=negotiated_type,
                                    exclude_expired=exclude_expired,
                                    as_of=as_of
                                )
                                state['baseline_rate_cache'][billing_code] = {
                                    'value': rate2_val,
                                    'billing_class': rate2_class,
                                    'meta': rate2_meta
                                }
                        baseline_stats = state['baseline_rate_cache'][billing_code]
                        description2 = baseline_data[billing_code]['description']

                        s1 = state['source1_rate_summary'][billing_code]

                        if compare_rule == 'all_classes':
                            s1_classes = (s1.get('classes') or {})
                            s2_classes = (baseline_stats.get('classes') or {})
                            all_classes = set(s1_classes.keys()) | set(s2_classes.keys())

                            # total_compared becomes number of code-class pairs that exist in both sources
                            for cls in all_classes:
                                key = f"{billing_code}|{cls}"
                                s1_entry = s1_classes.get(cls)
                                s2_val = s2_classes.get(cls)

                                if s1_entry is None or s2_val is None:
                                    continue

                                rate1_val = self._to_float(s1_entry.get('max', 0.0))
                                rate2_val = self._to_float(s2_val)

                                state['matched_code_classes'].add(key)
                                comparison['total_compared'] = len(state['matched_code_classes'])

                                diff = abs(rate1_val - rate2_val)
                                percent_diff = (diff / max(rate1_val, rate2_val) * 100) if max(rate1_val, rate2_val) > 0 else 0

                                comp_item = {
                                    'code': billing_code,
                                    'billing_class': cls,
                                    'source1_description': s1.get('description', description1),
                                    'source2_description': description2,
                                    'source1_rate': rate1_val,
                                    'source2_rate': rate2_val,
                                    'difference': rate1_val - rate2_val,
                                    'percent_difference': percent_diff,
                                    'rate_basis': 'all_classes_max'
                                }

                                prev_bucket = state['code_class_bucket'].get(key)
                                prev_diff = state['code_class_diff_cache'].get(key, 0.0)
                                if prev_bucket == 'higher_in_source1':
                                    comparison['higher_in_source1_count'] -= 1
                                    comparison['total_higher_in_source1_amount'] -= max(prev_diff, 0.0)
                                elif prev_bucket == 'higher_in_source2':
                                    comparison['higher_in_source2_count'] -= 1
                                    comparison['total_higher_in_source2_amount'] -= max(-prev_diff, 0.0)
                                elif prev_bucket == 'equal':
                                    comparison['equal_count'] -= 1

                                if rate1_val > rate2_val:
                                    bucket = 'higher_in_source1'
                                    comparison['higher_in_source1_count'] += 1
                                    comparison['total_higher_in_source1_amount'] += (rate1_val - rate2_val)
                                elif rate2_val > rate1_val:
                                    bucket = 'higher_in_source2'
                                    comparison['higher_in_source2_count'] += 1
                                    comparison['total_higher_in_source2_amount'] += (rate2_val - rate1_val)
                                else:
                                    bucket = 'equal'
                                    comparison['equal_count'] += 1

                                state['code_class_bucket'][key] = bucket
                                state['code_class_diff_cache'][key] = (rate1_val - rate2_val)

                                for bucket_name in ('higher_in_source1', 'higher_in_source2', 'equal'):
                                    if key in state['sample_by_bucket'][bucket_name] and bucket_name != bucket:
                                        del state['sample_by_bucket'][bucket_name][key]
                                if key in state['sample_by_bucket'][bucket]:
                                    state['sample_by_bucket'][bucket][key] = comp_item
                                elif len(state['sample_by_bucket'][bucket]) < self.incremental_sample_limit:
                                    state['sample_by_bucket'][bucket][key] = comp_item
                        else:
                            rate2_val = baseline_stats['value']
                            rate1_val, rate1_class, rate1_meta = self._finalize_source1_value(s1, compare_rule)

                            diff = abs(rate1_val - rate2_val)
                            percent_diff = (diff / max(rate1_val, rate2_val) * 100) if max(rate1_val, rate2_val) > 0 else 0

                            comp_item = {
                                'code': billing_code,
                                'source1_description': s1.get('description', description1),
                                'source2_description': description2,
                                'source1_rate': rate1_val,
                                'source2_rate': rate2_val,
                                'difference': rate1_val - rate2_val,
                                'percent_difference': percent_diff,
                                'source1_billing_class': rate1_class,
                                'source2_billing_class': baseline_stats.get('billing_class', 'unknown'),
                                'source1_rate_count': rate1_meta.get('count', 0),
                                'source2_rate_count': baseline_stats.get('meta', {}).get('count', 0),
                                'rate_basis': compare_rule
                            }

                            prev_bucket = state['code_bucket'].get(billing_code)
                            prev_diff = state['code_diff_cache'].get(billing_code, 0.0)
                            if prev_bucket == 'higher_in_source1':
                                comparison['higher_in_source1_count'] -= 1
                                comparison['total_higher_in_source1_amount'] -= max(prev_diff, 0.0)
                            elif prev_bucket == 'higher_in_source2':
                                comparison['higher_in_source2_count'] -= 1
                                comparison['total_higher_in_source2_amount'] -= max(-prev_diff, 0.0)
                            elif prev_bucket == 'equal':
                                comparison['equal_count'] -= 1

                            if rate1_val > rate2_val:
                                bucket = 'higher_in_source1'
                                comparison['higher_in_source1_count'] += 1
                                comparison['total_higher_in_source1_amount'] += (rate1_val - rate2_val)
                            elif rate2_val > rate1_val:
                                bucket = 'higher_in_source2'
                                comparison['higher_in_source2_count'] += 1
                                comparison['total_higher_in_source2_amount'] += (rate2_val - rate1_val)
                            else:
                                bucket = 'equal'
                                comparison['equal_count'] += 1

                            state['code_bucket'][billing_code] = bucket
                            state['code_diff_cache'][billing_code] = (rate1_val - rate2_val)

                            for bucket_name in ('higher_in_source1', 'higher_in_source2', 'equal'):
                                if billing_code in state['sample_by_bucket'][bucket_name] and bucket_name != bucket:
                                    del state['sample_by_bucket'][bucket_name][billing_code]
                            if billing_code in state['sample_by_bucket'][bucket]:
                                state['sample_by_bucket'][bucket][billing_code] = comp_item
                            elif len(state['sample_by_bucket'][bucket]) < self.incremental_sample_limit:
                                state['sample_by_bucket'][bucket][billing_code] = comp_item
                    else:
                        if billing_code not in state['only_in_source1_codes']:
                            state['only_in_source1_codes'].add(billing_code)
                            comparison['only_in_source1_count'] += 1

                            # Best-effort sample rate from this one item (avg of negotiated_prices).
                            item_sum = 0.0
                            item_count = 0
                            if 'negotiated_rates' in item:
                                for rate_info in item['negotiated_rates']:
                                    if 'negotiated_prices' in rate_info:
                                        for price in rate_info['negotiated_prices']:
                                            item_sum += self._to_float(price.get('negotiated_rate', 0))
                                            item_count += 1
                            item_avg = (item_sum / item_count) if item_count else 0.0

                            if len(comparison['only_in_source1_sample']) < self.incremental_only_in_source1_sample_limit:
                                comparison['only_in_source1_sample'].append({
                                    'code': billing_code,
                                    'description': description1,
                                    'rate': item_avg
                                })
        except Exception as e:
            return None, f"Error during incremental comparison: {str(e)}"

        comparison['higher_in_source1'] = list(state['sample_by_bucket']['higher_in_source1'].values())
        comparison['higher_in_source2'] = list(state['sample_by_bucket']['higher_in_source2'].values())
        comparison['equal'] = list(state['sample_by_bucket']['equal'].values())

        # Update baseline-only metrics (counts are accurate; sample is limited).
        comparison['only_in_source2_count'] = max(0, len(baseline_data) - len(state['matched_baseline_codes']))
        comparison['only_in_source2_sample'] = []
        for code, info in baseline_data.items():
            if code in state['matched_baseline_codes']:
                continue
            rate2_val, _, _ = self._rate_for_rule(
                info.get('rates', []),
                compare_rule,
                negotiated_type=negotiated_type,
                exclude_expired=exclude_expired,
                as_of=as_of
            )
            comparison['only_in_source2_sample'].append({
                'code': code,
                'description': info['description'],
                'rate': rate2_val
            })
            if len(comparison['only_in_source2_sample']) >= self.incremental_only_in_source2_sample_limit:
                break

        state['parts_processed'] += 1
        state['last_part'] = os.path.basename(part_path)
        state['updated_at'] = int(time.time())
        self._persist_incremental_session_summary(state)

        return self._incremental_state_to_payload(state), "Success"
    
    def extract_cpt_codes_from_index(self, data):
        """Extract in-network file URLs from index JSON"""
        urls = []
        if 'reporting_structure' in data:
            for structure in data['reporting_structure']:
                if 'in_network_files' in structure:
                    for file_info in structure['in_network_files']:
                        if 'location' in file_info:
                            urls.append({
                                'url': file_info['location'],
                                'description': file_info.get('description', 'Unknown')
                            })
        return urls

    def prepare_json_response(self, data, source_name):
        """Determine whether a loaded JSON is an index or direct in-network file"""
        urls = self.extract_cpt_codes_from_index(data)
        if urls:
            return {
                'success': True,
                'message': f'Loaded {len(urls)} in-network file references',
                'source_name': source_name,
                'type': 'json_index',
                'file_count': len(urls),
                'sample_urls': urls[:5]
            }

        in_network = data.get('in_network')
        if isinstance(in_network, list) and in_network:
            cpt_data = self.extract_cpt_pricing(data)
            self.cpt_pricing[source_name] = cpt_data
            base_payload = {
                'message': f'Loaded {len(cpt_data)} CPT codes directly from in-network JSON',
                'type': 'direct_in_network_json'
            }
            return self.build_cpt_response_payload(source_name, cpt_data, base_payload)

        return {
            'success': False,
            'message': 'JSON does not contain in-network file references or CPT pricing data.'
        }
    
    def load_json_from_path(self, path, source_name):
        """Load JSON file from disk, streaming if extremely large"""
        try:
            file_size = os.path.getsize(path)
            if file_size >= self.large_file_threshold:
                with self._open_json_stream(path) as stream:
                    cpt_data = self.extract_cpt_pricing_stream(stream)
                if not cpt_data:
                    return {
                        'success': False,
                        'message': 'Unable to locate in_network CPT data in the large JSON file.'
                    }
                self.cpt_pricing[source_name] = cpt_data
                self.data_sources[source_name] = {'source_type': 'direct_in_network_stream', 'path': path}
                base_payload = {
                    'message': f'Loaded {len(cpt_data)} CPT codes from large JSON via streaming',
                    'type': 'direct_in_network_json'
                }
                return self.build_cpt_response_payload(source_name, cpt_data, base_payload)
            else:
                with self._open_json_stream(path) as reader:
                    data = json.load(reader)
                self.data_sources[source_name] = data
                return self.prepare_json_response(data, source_name)
        except Exception as e:
            return {'success': False, 'message': f'Error processing JSON: {str(e)}'}

    def load_json_from_parts(self, part_paths, source_name):
        """Load JSON from multiple sequential parts without concatenating on disk."""
        try:
            if not part_paths:
                return {'success': False, 'message': 'No parts provided for loading.'}

            with self._open_json_stream(part_paths) as stream:
                cpt_data = self.extract_cpt_pricing_stream(stream)

            if not cpt_data:
                return {
                    'success': False,
                    'message': 'Unable to locate in_network CPT data in the provided parts.'
                }

            self.cpt_pricing[source_name] = cpt_data
            self.data_sources[source_name] = {'source_type': 'direct_in_network_stream_parts', 'paths': part_paths}
            base_payload = {
                'message': f'Loaded {len(cpt_data)} CPT codes from {len(part_paths)} parts (streamed)',
                'type': 'direct_in_network_json'
            }
            return self.build_cpt_response_payload(source_name, cpt_data, base_payload)
        except Exception as e:
            return {'success': False, 'message': f'Error processing multipart JSON: {str(e)}'}

    def _open_json_stream(self, path):
        # Accept a single file path or a list of file parts
        if isinstance(path, (list, tuple)):
            return MultiPartStream(list(path))
        if path.endswith('.gz'):
            return gzip.open(path, 'rb')
        return open(path, 'rb')

    def extract_cpt_pricing_stream(self, stream, max_codes=None, skip_codes=0):
        """Stream large JSON files to build CPT pricing without loading entire document using ijson"""
        cpt_data = {}
        count = 0
        skipped = 0
        
        try:
            parser = ijson.items(stream, 'in_network.item')
            
            for item in parser:
                # Skip items if pagination offset is specified
                if skip_codes > 0 and skipped < skip_codes:
                    if item.get('billing_code_type') == 'CPT' and item.get('billing_code'):
                        skipped += 1
                    continue
                    
                if self._add_cpt_entry(item, cpt_data):
                    count += 1
                    if max_codes is not None and count >= max_codes:
                        break
                        
        except Exception as e:
            print(f"Error streaming JSON: {str(e)}")
            
        return cpt_data
    
    def extract_cpt_pricing_paginated(self, file_path, page=1, page_size=500):
        """Extract CPT pricing with pagination support"""
        skip = (page - 1) * page_size
        
        try:
            with self._open_json_stream(file_path) as stream:
                cpt_data = self.extract_cpt_pricing_stream(stream, max_codes=page_size, skip_codes=skip)
            
            return {
                'success': True,
                'page': page,
                'page_size': page_size,
                'cpt_count': len(cpt_data),
                'cpt_data': cpt_data,
                'has_more': len(cpt_data) == page_size  # If we got full page, likely more data
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error loading page {page}: {str(e)}'
            }
    
    def build_cpt_response_payload(self, source_name, cpt_data, base_payload=None):
        """Create a response payload with preview data if needed to avoid huge responses"""
        payload = base_payload.copy() if base_payload else {}
        payload.update({
            'success': True,
            'source_name': source_name,
            'cpt_count': len(cpt_data),
            'preview_limit': self.preview_limit,
            'preview_only': False
        })

        if len(cpt_data) <= self.preview_limit:
            payload['cpt_data'] = cpt_data
        else:
            preview = {}
            for idx, (code, info) in enumerate(cpt_data.items()):
                if idx >= self.preview_limit:
                    break
                preview[code] = info
            payload['cpt_preview'] = preview
            payload['preview_only'] = True
            payload['preview_message'] = (
                f'Showing the first {self.preview_limit} CPT codes in the browser. '
                'Use the CSV export to download the full dataset.'
            )

        return payload
    
    def fetch_and_parse_gzipped_json(self, url):
        """Fetch and parse gzipped JSON file with simple caching"""
        cache_hit = False
        cache_path = None
        content = None

        try:
            if url.startswith('http'):
                parsed_name = os.path.basename(url.split('?')[0]) or 'file'
                url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
                cache_filename = f"{url_hash}_{parsed_name}"
                cache_path = os.path.join(self.cache_dir, cache_filename)

                if os.path.exists(cache_path):
                    cache_hit = True
                    with open(cache_path, 'rb') as f:
                        content = f.read()
            else:
                if os.path.exists(url):
                    with open(url, 'rb') as f:
                        content = f.read()
                else:
                    print(f"Error: File not found - {url}")
                    return None, cache_hit

            if content is None:
                response = requests.get(url, timeout=60)

                # Check for 403 Forbidden which usually means expired link
                if response.status_code == 403:
                    if "AccessDenied" in response.text or "Expired" in response.text:
                        print(f"Error: URL is expired or access denied: {url}")
                        return "EXPIRED", cache_hit

                response.raise_for_status()
                content = response.content

                if cache_path:
                    try:
                        with open(cache_path, 'wb') as f:
                            f.write(content)
                    except OSError as e:
                        print(f"Warning: Unable to write cache file {cache_path}: {e}")

            # Try to decompress as gzip first
            try:
                with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                    data = json.load(gz)
                return data, cache_hit
            except (gzip.BadGzipFile, OSError):
                # If not gzipped, try as regular JSON
                try:
                    data = json.loads(content)
                    return data, cache_hit
                except json.JSONDecodeError:
                    print("Error: Response is neither gzipped JSON nor regular JSON")
                    preview = content[:100] if content else b''
                    print(f"First 100 bytes: {preview}")
                    return None, cache_hit
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None, cache_hit
        except Exception as e:
            print(f"Unexpected error fetching {url}: {e}")
            return None, cache_hit
    
    def _add_cpt_entry(self, item, cpt_data):
        """Add a single CPT entry into the aggregated dictionary"""
        billing_code_type = item.get('billing_code_type', '')
        billing_code = item.get('billing_code', '')
        billing_code = str(billing_code).strip() if billing_code is not None else ''

        if billing_code_type != 'CPT' or not billing_code:
            return False

        rates = []
        if 'negotiated_rates' in item:
            for rate_info in item['negotiated_rates']:
                if 'negotiated_prices' in rate_info:
                    for price in rate_info['negotiated_prices']:
                        rates.append({
                            'billing_class': price.get('billing_class', 'unknown'),
                            'negotiated_rate': price.get('negotiated_rate', 0),
                            'billing_code_modifier': price.get('billing_code_modifier', []),
                            'negotiated_type': price.get('negotiated_type', ''),
                            'expiration_date': price.get('expiration_date'),
                            'service_code': price.get('service_code', [])
                        })

        description = item.get('description', 'No description')

        if billing_code in cpt_data:
            existing = cpt_data[billing_code]
            existing['rates'].extend(rates)
            if existing.get('description') in (None, '', 'No description') and description not in (None, '', 'No description'):
                existing['description'] = description
            return False

        cpt_data[billing_code] = {
            'description': description,
            'rates': rates
        }
        return True

    def extract_cpt_pricing(self, data, max_codes=None):
        """Extract CPT codes and pricing from in-network file"""
        cpt_data = {}
        count = 0
        
        if 'in_network' in data:
            for item in data['in_network']:
                if max_codes is not None and count >= max_codes:
                    break
                if self._add_cpt_entry(item, cpt_data):
                    count += 1
        
        return cpt_data
    
    def compare_pricing(self, source1_name, source2_name, compare_rule='max', negotiated_type=None, exclude_expired=False, as_of=None):
        """Compare pricing between two sources"""
        if source1_name not in self.cpt_pricing or source2_name not in self.cpt_pricing:
            return None
        
        source1_data = self.cpt_pricing[source1_name]
        source2_data = self.cpt_pricing[source2_name]
        
        compare_rule = (compare_rule or 'max').strip().lower()

        if compare_rule == 'all_classes':
            return self._compare_pricing_all_classes(source1_name, source2_name, negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)

        if compare_rule == 'per_occurrence':
            return self._compare_pricing_per_occurrence(source1_name, source2_name, negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)

        if compare_rule == 'context':
            return self._compare_pricing_by_context(source1_name, source2_name, negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)

        comparison = {
            'source1': source1_name,
            'source2': source2_name,
            'compare_rule': compare_rule,
            'higher_in_source1': [],
            'higher_in_source2': [],
            'equal': [],
            'only_in_source1': [],
            'only_in_source2': [],
            'total_compared': 0,
            'total_source1': len(source1_data),
            'total_source2': len(source2_data),
            'total_higher_in_source1_amount': 0,
            'total_higher_in_source2_amount': 0
        }
        
        # Compare common CPT codes
        all_codes = set(source1_data.keys()) | set(source2_data.keys())
        
        for code in all_codes:
            if code in source1_data and code in source2_data:
                # Get average rates
                rate1, _, _ = self._rate_for_rule(source1_data[code].get('rates', []), compare_rule, negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)
                rate2, _, _ = self._rate_for_rule(source2_data[code].get('rates', []), compare_rule, negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)
                
                comparison['total_compared'] += 1
                
                diff = abs(rate1 - rate2)
                percent_diff = (diff / max(rate1, rate2) * 100) if max(rate1, rate2) > 0 else 0
                
                desc1 = source1_data[code]['description']
                desc2 = source2_data[code]['description']
                descriptions_match = (desc1 or '').strip() == (desc2 or '').strip()

                item = {
                    'code': code,
                    'source1_description': desc1,
                    'source2_description': desc2,
                    'descriptions_match': descriptions_match,
                    'source1_rate': rate1,
                    'source2_rate': rate2,
                    'difference': rate1 - rate2,
                    'percent_difference': percent_diff
                }
                
                if rate1 > rate2:
                    comparison['higher_in_source1'].append(item)
                    comparison['total_higher_in_source1_amount'] += (rate1 - rate2)
                elif rate2 > rate1:
                    comparison['higher_in_source2'].append(item)
                    comparison['total_higher_in_source2_amount'] += (rate2 - rate1)
                else:
                    comparison['equal'].append(item)
                    
            elif code in source1_data:
                comparison['only_in_source1'].append({
                    'code': code,
                    'description': source1_data[code]['description'],
                    'rate': source1_data[code]['rates'][0]['negotiated_rate'] if source1_data[code]['rates'] else 0
                })
            else:
                comparison['only_in_source2'].append({
                    'code': code,
                    'description': source2_data[code]['description'],
                    'rate': source2_data[code]['rates'][0]['negotiated_rate'] if source2_data[code]['rates'] else 0
                })
        
        return comparison

    def _compare_pricing_by_context(self, source1_name, source2_name, negotiated_type=None, exclude_expired=False, as_of=None):
        """Compare per CPT per (billing_class + modifier) context using max rate in that context."""
        if source1_name not in self.cpt_pricing or source2_name not in self.cpt_pricing:
            return None

        source1_data = self.cpt_pricing[source1_name]
        source2_data = self.cpt_pricing[source2_name]

        s1_lookup = {str(k).strip(): v for k, v in source1_data.items()}
        s2_lookup = {str(k).strip(): v for k, v in source2_data.items()}

        all_codes = set(s1_lookup.keys()) | set(s2_lookup.keys())

        comparison = {
            'source1': source1_name,
            'source2': source2_name,
            'compare_rule': 'context',
            'higher_in_source1': [],
            'higher_in_source2': [],
            'equal': [],
            'only_in_source1': [],
            'only_in_source2': [],
            'total_compared': 0,
            'total_source1': len(s1_lookup),
            'total_source2': len(s2_lookup),
            'total_higher_in_source1_amount': 0,
            'total_higher_in_source2_amount': 0
        }

        for code in all_codes:
            s1 = s1_lookup.get(code)
            s2 = s2_lookup.get(code)

            if s1 and s2:
                s1_rates = self._filter_rates(s1.get('rates', []), negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)
                s2_rates = self._filter_rates(s2.get('rates', []), negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)
                s1_ctx, _ = self._max_rate_by_context(s1_rates)
                s2_ctx, _ = self._max_rate_by_context(s2_rates)
                all_ctx = set(s1_ctx.keys()) | set(s2_ctx.keys())

                for (billing_class, modifiers) in all_ctx:
                    if (billing_class, modifiers) in s1_ctx and (billing_class, modifiers) in s2_ctx:
                        rate1 = self._to_float(s1_ctx[(billing_class, modifiers)])
                        rate2 = self._to_float(s2_ctx[(billing_class, modifiers)])
                        comparison['total_compared'] += 1

                        diff = abs(rate1 - rate2)
                        percent_diff = (diff / max(rate1, rate2) * 100) if max(rate1, rate2) > 0 else 0

                        item = {
                            'code': code,
                            'billing_class': billing_class,
                            'modifiers': list(modifiers),
                            'source1_description': s1.get('description', ''),
                            'source2_description': s2.get('description', ''),
                            'source1_rate': rate1,
                            'source2_rate': rate2,
                            'difference': rate1 - rate2,
                            'percent_difference': percent_diff
                        }

                        if rate1 > rate2:
                            comparison['higher_in_source1'].append(item)
                            comparison['total_higher_in_source1_amount'] += (rate1 - rate2)
                        elif rate2 > rate1:
                            comparison['higher_in_source2'].append(item)
                            comparison['total_higher_in_source2_amount'] += (rate2 - rate1)
                        else:
                            comparison['equal'].append(item)
                    elif (billing_class, modifiers) in s1_ctx:
                        comparison['only_in_source1'].append({
                            'code': code,
                            'billing_class': billing_class,
                            'modifiers': list(modifiers),
                            'description': s1.get('description', ''),
                            'rate': self._to_float(s1_ctx[(billing_class, modifiers)])
                        })
                    else:
                        comparison['only_in_source2'].append({
                            'code': code,
                            'billing_class': billing_class,
                            'modifiers': list(modifiers),
                            'description': s2.get('description', ''),
                            'rate': self._to_float(s2_ctx[(billing_class, modifiers)])
                        })
            elif s1:
                s1_rates = self._filter_rates(s1.get('rates', []), negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)
                s1_ctx, _ = self._max_rate_by_context(s1_rates)
                for (billing_class, modifiers), rate in s1_ctx.items():
                    comparison['only_in_source1'].append({
                        'code': code,
                        'billing_class': billing_class,
                        'modifiers': list(modifiers),
                        'description': s1.get('description', ''),
                        'rate': self._to_float(rate)
                    })
            elif s2:
                s2_rates = self._filter_rates(s2.get('rates', []), negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)
                s2_ctx, _ = self._max_rate_by_context(s2_rates)
                for (billing_class, modifiers), rate in s2_ctx.items():
                    comparison['only_in_source2'].append({
                        'code': code,
                        'billing_class': billing_class,
                        'modifiers': list(modifiers),
                        'description': s2.get('description', ''),
                        'rate': self._to_float(rate)
                    })

        return comparison

    def _compare_pricing_per_occurrence(self, source1_name, source2_name, negotiated_type=None, exclude_expired=False, as_of=None):
        """Compare per-code highest occurrence against baseline max (does not multiply-count repeated occurrences)."""
        if source1_name not in self.cpt_pricing or source2_name not in self.cpt_pricing:
            return None

        source1_data = self.cpt_pricing[source1_name]
        source2_data = self.cpt_pricing[source2_name]

        comparison = {
            'source1': source1_name,
            'source2': source2_name,
            'compare_rule': 'per_occurrence',
            'higher_in_source1': [],
            'higher_in_source2': [],
            'equal': [],
            'only_in_source1': [],
            'only_in_source2': [],
            'total_compared': 0,
            'total_source1': len(source1_data),
            'total_source2': len(source2_data),
            'total_higher_in_source1_amount': 0,
            'total_higher_in_source2_amount': 0
        }

        # Cache baseline (source2) max per code
        baseline_max = {}
        for code, info in source2_data.items():
            code_str = str(code).strip()
            rate2, _, _ = self._rate_for_rule(info.get('rates', []), 'max', negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)
            baseline_max[code_str] = rate2

        for code, info in source1_data.items():
            code_str = str(code).strip()
            if code_str not in baseline_max:
                # cannot compare occurrences; mark only-in-source1 by code (not per occurrence)
                comparison['only_in_source1'].append({
                    'code': code_str,
                    'description': info.get('description', ''),
                    'rate': self._max_rate_with_class(info.get('rates', [])).get('max', 0.0)
                })
                continue

            rate2 = baseline_max[code_str]
            filtered = self._filter_rates(info.get('rates', []) or [], negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)
            max_info = self._max_rate_with_class(filtered)
            rate1 = max_info.get('max', 0.0)
            billing_class = max_info.get('billing_class', 'unknown')

            comparison['total_compared'] += 1
            diff = abs(rate1 - rate2)
            percent_diff = (diff / max(rate1, rate2) * 100) if max(rate1, rate2) > 0 else 0

            item = {
                'code': code_str,
                'billing_class': billing_class,
                'source1_description': info.get('description', ''),
                'source2_description': source2_data.get(code_str, {}).get('description', ''),
                'source1_rate': rate1,
                'source2_rate': rate2,
                'difference': rate1 - rate2,
                'percent_difference': percent_diff
            }

            if rate1 > rate2:
                comparison['higher_in_source1'].append(item)
                comparison['total_higher_in_source1_amount'] += (rate1 - rate2)
            elif rate2 > rate1:
                comparison['higher_in_source2'].append(item)
                comparison['total_higher_in_source2_amount'] += (rate2 - rate1)
            else:
                comparison['equal'].append(item)

        return comparison

    def _compare_pricing_all_classes(self, source1_name, source2_name, negotiated_type=None, exclude_expired=False, as_of=None):
        """Compare per CPT per billing_class (max per class)."""
        if source1_name not in self.cpt_pricing or source2_name not in self.cpt_pricing:
            return None

        source1_data = self.cpt_pricing[source1_name]
        source2_data = self.cpt_pricing[source2_name]

        comparison = {
            'source1': source1_name,
            'source2': source2_name,
            'compare_rule': 'all_classes',
            'higher_in_source1': [],
            'higher_in_source2': [],
            'equal': [],
            'only_in_source1': [],
            'only_in_source2': [],
            'total_compared': 0,
            'total_source1': len(source1_data),
            'total_source2': len(source2_data),
            'total_higher_in_source1_amount': 0,
            'total_higher_in_source2_amount': 0
        }

        all_codes = set(source1_data.keys()) | set(source2_data.keys())

        for code in all_codes:
            s1 = source1_data.get(code)
            s2 = source2_data.get(code)

            if s1 and s2:
                s1_rates = self._filter_rates(s1.get('rates', []), negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)
                s2_rates = self._filter_rates(s2.get('rates', []), negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)
                s1_classes, _ = self._max_rate_by_class(s1_rates)
                s2_classes, _ = self._max_rate_by_class(s2_rates)
                all_classes = set(s1_classes.keys()) | set(s2_classes.keys())

                for cls in all_classes:
                    if cls in s1_classes and cls in s2_classes:
                        rate1 = self._to_float(s1_classes[cls])
                        rate2 = self._to_float(s2_classes[cls])
                        comparison['total_compared'] += 1

                        diff = abs(rate1 - rate2)
                        percent_diff = (diff / max(rate1, rate2) * 100) if max(rate1, rate2) > 0 else 0

                        item = {
                            'code': code,
                            'billing_class': cls,
                            'source1_description': s1.get('description', ''),
                            'source2_description': s2.get('description', ''),
                            'source1_rate': rate1,
                            'source2_rate': rate2,
                            'difference': rate1 - rate2,
                            'percent_difference': percent_diff
                        }

                        if rate1 > rate2:
                            comparison['higher_in_source1'].append(item)
                            comparison['total_higher_in_source1_amount'] += (rate1 - rate2)
                        elif rate2 > rate1:
                            comparison['higher_in_source2'].append(item)
                            comparison['total_higher_in_source2_amount'] += (rate2 - rate1)
                        else:
                            comparison['equal'].append(item)
                    elif cls in s1_classes:
                        comparison['only_in_source1'].append({
                            'code': code,
                            'billing_class': cls,
                            'description': s1.get('description', ''),
                            'rate': self._to_float(s1_classes[cls])
                        })
                    else:
                        comparison['only_in_source2'].append({
                            'code': code,
                            'billing_class': cls,
                            'description': s2.get('description', ''),
                            'rate': self._to_float(s2_classes[cls])
                        })
            elif s1:
                s1_rates = self._filter_rates(s1.get('rates', []), negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)
                s1_classes, _ = self._max_rate_by_class(s1_rates)
                for cls, rate in s1_classes.items():
                    comparison['only_in_source1'].append({
                        'code': code,
                        'billing_class': cls,
                        'description': s1.get('description', ''),
                        'rate': self._to_float(rate)
                    })
            elif s2:
                s2_rates = self._filter_rates(s2.get('rates', []), negotiated_type=negotiated_type, exclude_expired=exclude_expired, as_of=as_of)
                s2_classes, _ = self._max_rate_by_class(s2_rates)
                for cls, rate in s2_classes.items():
                    comparison['only_in_source2'].append({
                        'code': code,
                        'billing_class': cls,
                        'description': s2.get('description', ''),
                        'rate': self._to_float(rate)
                    })

        return comparison

    def stream_compare(self, large_file_path, baseline_source_name):
        """Compare a large file (Source 1) against a loaded baseline (Source 2) using streaming."""
        if baseline_source_name not in self.cpt_pricing:
            return None, "Baseline source not loaded."
        
        baseline_data = self.cpt_pricing[baseline_source_name]
        
        comparison = {
            'source1': 'Large File Import',
            'source2': baseline_source_name,
            'higher_in_source1': [],
            'higher_in_source2': [],
            'equal': [],
            'only_in_source1_count': 0, # Storing count only to save RAM
            'only_in_source1_sample': [], # Store first 100 as sample
            'only_in_source2': [], # Will be calculated at end
            'total_compared': 0,
            'total_source1_count': 0,
            'total_source2': len(baseline_data),
            'total_higher_in_source1_amount': 0,
            'total_higher_in_source2_amount': 0
        }
        
        # Track which codes from baseline were matched
        matched_baseline_codes = set()
        
        try:
            with self._open_json_stream(large_file_path) as stream:
                parser = ijson.items(stream, 'in_network.item')
                
                for item in parser:
                    billing_code = item.get('billing_code')
                    billing_code = str(billing_code).strip() if billing_code is not None else ''
                    if not billing_code or item.get('billing_code_type') != 'CPT':
                        continue
                        
                    comparison['total_source1_count'] += 1
                    
                    # Get rate for this item from large file
                    rate1 = 0.0
                    if 'negotiated_rates' in item:
                        for rate_info in item['negotiated_rates']:
                            if 'negotiated_prices' in rate_info:
                                for price in rate_info['negotiated_prices']:
                                    rate1 = self._to_float(price.get('negotiated_rate', 0))
                                    break # Take first for simplicity
                            if rate1 > 0: break
                    
                    description1 = item.get('description', 'No description')
                    
                    if billing_code in baseline_data:
                        matched_baseline_codes.add(billing_code)
                        comparison['total_compared'] += 1
                        
                        rate2 = self._to_float(baseline_data[billing_code]['rates'][0]['negotiated_rate']) if baseline_data[billing_code]['rates'] else 0.0
                        description2 = baseline_data[billing_code]['description']
                        
                        diff = abs(rate1 - rate2)
                        percent_diff = (diff / max(rate1, rate2) * 100) if max(rate1, rate2) > 0 else 0
                        
                        comp_item = {
                            'code': billing_code,
                            'source1_description': description1,
                            'source2_description': description2,
                            'source1_rate': rate1,
                            'source2_rate': rate2,
                            'difference': rate1 - rate2,
                            'percent_difference': percent_diff
                        }
                        
                        if rate1 > rate2:
                            comparison['higher_in_source1'].append(comp_item)
                            comparison['total_higher_in_source1_amount'] += (rate1 - rate2)
                        elif rate2 > rate1:
                            comparison['higher_in_source2'].append(comp_item)
                            comparison['total_higher_in_source2_amount'] += (rate2 - rate1)
                        else:
                            comparison['equal'].append(comp_item)
                    else:
                        comparison['only_in_source1_count'] += 1
                        if len(comparison['only_in_source1_sample']) < 100:
                            comparison['only_in_source1_sample'].append({
                                'code': billing_code,
                                'description': description1,
                                'rate': rate1
                            })
                            
        except Exception as e:
            return None, f"Error during stream comparison: {str(e)}"
            
        # Identify codes only in baseline
        for code, info in baseline_data.items():
            if code not in matched_baseline_codes:
                comparison['only_in_source2'].append({
                    'code': code,
                    'description': info['description'],
                    'rate': self._to_float(info['rates'][0]['negotiated_rate']) if info['rates'] else 0.0
                })
                
        return comparison, "Success"

    def compare_paginated(self, file_path, baseline_source_name, page=1, page_size=500):
        """Compare a page of large file against baseline source"""
        if baseline_source_name not in self.cpt_pricing:
            return None, "Baseline source not loaded."
        
        baseline_data = self.cpt_pricing[baseline_source_name]
        skip = (page - 1) * page_size
        
        comparison = {
            'source1': 'Large File (Page)',
            'source2': baseline_source_name,
            'page': page,
            'page_size': page_size,
            'higher_in_source1': [],
            'higher_in_source2': [],
            'equal': [],
            'only_in_source1': [],
            'only_in_source2_sample': [],  # Sample only for this page
            'total_compared': 0,
            'total_in_page': 0
        }
        
        try:
            with self._open_json_stream(file_path) as stream:
                parser = ijson.items(stream, 'in_network.item')
                
                skipped = 0
                processed = 0
                
                for item in parser:
                    billing_code = item.get('billing_code')
                    billing_code = str(billing_code).strip() if billing_code is not None else ''
                    if not billing_code or item.get('billing_code_type') != 'CPT':
                        continue
                    
                    # Skip to the right page
                    if skipped < skip:
                        skipped += 1
                        continue
                    
                    # Stop after page_size records
                    if processed >= page_size:
                        break
                    
                    processed += 1
                    comparison['total_in_page'] += 1
                    
                    # Get rate from large file
                    rate1 = 0.0
                    if 'negotiated_rates' in item:
                        for rate_info in item['negotiated_rates']:
                            if 'negotiated_prices' in rate_info:
                                for price in rate_info['negotiated_prices']:
                                    rate1 = self._to_float(price.get('negotiated_rate', 0))
                                    break
                            if rate1 > 0: break
                    
                    description1 = item.get('description', 'No description')
                    
                    if billing_code in baseline_data:
                        comparison['total_compared'] += 1
                        
                        rate2 = self._to_float(baseline_data[billing_code]['rates'][0]['negotiated_rate']) if baseline_data[billing_code]['rates'] else 0.0
                        description2 = baseline_data[billing_code]['description']
                        
                        diff = abs(rate1 - rate2)
                        percent_diff = (diff / max(rate1, rate2) * 100) if max(rate1, rate2) > 0 else 0
                        
                        comp_item = {
                            'code': billing_code,
                            'source1_description': description1,
                            'source2_description': description2,
                            'source1_rate': rate1,
                            'source2_rate': rate2,
                            'difference': rate1 - rate2,
                            'percent_difference': percent_diff
                        }
                        
                        if rate1 > rate2:
                            comparison['higher_in_source1'].append(comp_item)
                        elif rate2 > rate1:
                            comparison['higher_in_source2'].append(comp_item)
                        else:
                            comparison['equal'].append(comp_item)
                    else:
                        comparison['only_in_source1'].append({
                            'code': billing_code,
                            'description': description1,
                            'rate': rate1
                        })
                        
        except Exception as e:
            return None, f"Error during paginated comparison: {str(e)}"
        
        # Add sample of codes only in baseline (first 50)
        baseline_only_count = 0
        for code, info in baseline_data.items():
            if baseline_only_count >= 50:
                break
            comparison['only_in_source2_sample'].append({
                'code': code,
                'description': info['description'],
                'rate': self._to_float(info['rates'][0]['negotiated_rate']) if info['rates'] else 0.0
            })
            baseline_only_count += 1
        
        return comparison, "Success"


analyzer = CPTPricingAnalyzer()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload or URL"""
    try:
        source_name = request.form.get('source_name', 'Source')
        
        # Check if file was uploaded
        if 'file' in request.files and request.files['file'].filename:
            file = request.files['file']
            filename = file.filename.lower()
            temp_path = analyzer.save_uploaded_file(file, 'source')
            
            # Check if it's an Excel file
            if filename.endswith(('.xlsx', '.xls')):
                success, message = analyzer.load_excel_file(temp_path, source_name)
                
                if success:
                    cpt_count = len(analyzer.cpt_pricing.get(source_name, {}))
                    return jsonify({
                        'success': True,
                        'message': message,
                        'source_name': source_name,
                        'type': 'excel',
                        'cpt_count': cpt_count
                    })
                else:
                    return jsonify({'success': False, 'message': message})
            
            elif filename.endswith('.csv'):
                success, message, cpt_data = analyzer.load_csv_file(temp_path, source_name)
                if success:
                    base_payload = {
                        'message': message,
                        'type': 'csv'
                    }
                    response_payload = analyzer.build_cpt_response_payload(source_name, cpt_data, base_payload)
                    return jsonify(response_payload)
                return jsonify({'success': False, 'message': message})
            
            # Otherwise treat as JSON
            else:
                # Check file size for smart loading
                file_size = os.path.getsize(temp_path)
                file_size_mb = file_size / (1024 * 1024)
                
                # Auto-load if under 300MB, suggest pagination if over
                if file_size_mb < 300:
                    # Small enough - load completely
                    response_payload = analyzer.load_json_from_path(temp_path, source_name)
                    if response_payload.get('success'):
                        response_payload['auto_loaded'] = True
                        response_payload['file_size_mb'] = round(file_size_mb, 2)
                        response_payload['message'] = f"Auto-loaded {round(file_size_mb, 2)}MB file. " + response_payload.get('message', '')
                    return jsonify(response_payload)
                else:
                    # Too large - suggest pagination
                    # Store file for pagination
                    file_id = hashlib.sha256(temp_path.encode()).hexdigest()[:16]
                    analyzer.data_sources[f'_large_{file_id}'] = {
                        'path': temp_path,
                        'type': 'large_json_file',
                        'source_name': source_name
                    }
                    
                    return jsonify({
                        'success': True,
                        'file_size_mb': round(file_size_mb, 2),
                        'file_id': file_id,
                        'source_name': source_name,
                        'type': 'large_file_pagination_recommended',
                        'message': f'File is {round(file_size_mb, 2)}MB (> 300MB). Use pagination for better performance.',
                        'recommendation': 'Use /load_paginated endpoint with this file_id for page-by-page loading.',
                        'auto_loaded': False
                    })
        
        # Check if URL was provided
        elif request.form.get('url'):
            url = request.form.get('url')
            success, message = analyzer.load_json_file(url, source_name)
            
            if success:
                data = analyzer.data_sources[source_name]
                response_payload = analyzer.prepare_json_response(data, source_name)
                if 'message' not in response_payload or not response_payload['message']:
                    response_payload['message'] = message
                return jsonify(response_payload)
            else:
                return jsonify({'success': False, 'message': message})
        
        return jsonify({'success': False, 'message': 'No file or URL provided'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/fetch_pricing', methods=['POST'])
def fetch_pricing():
    """Fetch actual pricing data from a specific URL"""
    try:
        url = request.json.get('url')
        source_name = request.json.get('source_name', 'Source')
        
        # Fetch and parse the gzipped JSON
        data, cache_hit = analyzer.fetch_and_parse_gzipped_json(url)
        
        if data == "EXPIRED":
            return jsonify({
                'success': False, 
                'message': 'The link has expired. These secure links usually expire after a set time. Please download a fresh index file from the insurance provider website.'
            })
        elif data:
            # Extract CPT pricing
            cpt_data = analyzer.extract_cpt_pricing(data)
            
            # Store for comparison
            analyzer.cpt_pricing[source_name] = cpt_data
            
            payload = analyzer.build_cpt_response_payload(source_name, cpt_data, {
                'type': 'fetched_in_network'
            })
            payload['cache_hit'] = cache_hit
            return jsonify(payload)
        else:
            return jsonify({'success': False, 'message': 'Failed to fetch pricing data. The URL may be invalid or the server is unreachable.'})
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/load_test_data', methods=['POST'])
def load_test_data():
    """Load test data for demo purposes"""
    try:
        source_name = request.json.get('source_name', 'Test Insurance')
        
        # Load test data from file
        with open('test_pricing_data.json', 'r') as f:
            data = json.load(f)
        
        # Extract CPT pricing
        cpt_data = analyzer.extract_cpt_pricing(data)
        
        # Store for comparison
        analyzer.cpt_pricing[source_name] = cpt_data
        
        return jsonify({
            'success': True,
            'cpt_count': len(cpt_data),
            'cpt_data': cpt_data,
            'source_name': source_name
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/compare', methods=['POST'])
def compare():
    """Compare pricing between sources"""
    try:
        source1 = request.json.get('source1')
        source2 = request.json.get('source2')
        compare_rule = (request.json.get('compare_rule') or request.json.get('rule') or 'max').strip().lower()
        negotiated_type = (request.json.get('negotiated_type') or '').strip()
        exclude_expired = bool(request.json.get('exclude_expired') or False)

        # Make sure both sources actually have CPT pricing loaded
        missing_sources = []
        if not source1 or source1 not in analyzer.cpt_pricing:
            missing_sources.append(source1 or 'Source 1')
        if not source2 or source2 not in analyzer.cpt_pricing:
            missing_sources.append(source2 or 'Source 2')

        if missing_sources:
            missing_text = ', '.join(missing_sources)
            return jsonify({
                'success': False,
                'message': f"Pricing data hasn't been loaded for: {missing_text}. After uploading an index JSON, click one of the in-network file links (or load an Excel/Test data source) so the detailed CPT pricing can be pulled before comparing."
            })

        comparison = analyzer.compare_pricing(
            source1,
            source2,
            compare_rule=compare_rule,
            negotiated_type=negotiated_type or None,
            exclude_expired=exclude_expired
        )

        if comparison:
            return jsonify({'success': True, 'comparison': comparison})
        else:
            return jsonify({'success': False, 'message': 'Sources not found'})
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/stream_compare_upload', methods=['POST'])
def stream_compare_upload():
    """Handle large file upload for direct streaming comparison"""
    try:
        baseline_source = request.form.get('baseline_source')
        if not baseline_source:
             return jsonify({'success': False, 'message': 'Please select a baseline source (Source 2) first.'})

        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'No file uploaded.'})
            
        file = request.files['file']
        if not file.filename:
            return jsonify({'success': False, 'message': 'No file selected.'})
            
        # Save temp file
        temp_path = analyzer.save_uploaded_file(file, 'stream_compare')
        
        # valid json?
        if not temp_path.endswith('.json') and not temp_path.endswith('.gz'):
             return jsonify({'success': False, 'message': 'Only JSON or GZIP files supported for stream comparison.'})

        comparison, msg = analyzer.stream_compare(temp_path, baseline_source)
        
        if comparison:
            # Clean up temp file to save space? Maybe keep for a bit?
            # os.remove(temp_path) 
            return jsonify({'success': True, 'comparison': comparison, 'message': 'Stream comparison complete.'})
        else:
            return jsonify({'success': False, 'message': msg})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/upload_multipart_part', methods=['POST'])
def upload_multipart_part():
    """Upload one chunk/part of a split JSON and keep it in a session."""
    try:
        source_name = request.form.get('source_name', 'Source 1 (parts)')
        session_id = request.form.get('session_id')
        baseline_source = request.form.get('baseline_source')
        compare_rule = request.form.get('compare_rule', 'max')
        negotiated_type = (request.form.get('negotiated_type') or '').strip()
        exclude_expired_raw = (request.form.get('exclude_expired') or '').strip().lower()
        exclude_expired = exclude_expired_raw in ('1', 'true', 'yes', 'on')

        if 'file' not in request.files or not request.files['file'].filename:
            return jsonify({'success': False, 'message': 'No part file uploaded.'})

        file = request.files['file']
        session_id, part_path, part_num, is_duplicate, original_name = analyzer.add_multipart_part(session_id, file, source_name)
        all_paths = analyzer.get_multipart_paths(session_id)
        total_size_mb = round(sum(os.path.getsize(p) for p in all_paths) / (1024 * 1024), 2)
        last_part_size_mb = round(os.path.getsize(part_path) / (1024 * 1024), 2) if part_path else 0.0

        payload = {
            'success': True,
            'session_id': session_id,
            'source_name': analyzer.multipart_sessions[session_id]['source_name'],
            'part_count': part_num,
            'last_part_size_mb': last_part_size_mb,
            'total_size_mb': total_size_mb,
            'message': f'Part {part_num} uploaded ({last_part_size_mb} MB). {len(all_paths)} parts stored.'
        }

        if is_duplicate:
            payload['duplicate'] = True
            display_name = original_name or '(unknown)'
            payload['message'] = f'Duplicate part ignored: {display_name}.'
            return jsonify(payload)

        if baseline_source:
            comparison, msg = analyzer.incremental_compare_part(
                session_id=session_id,
                part_path=part_path,
                source1_name=payload['source_name'],
                baseline_source_name=baseline_source,
                compare_rule=compare_rule,
                negotiated_type=negotiated_type or None,
                exclude_expired=exclude_expired
            )
            if not comparison:
                return jsonify({'success': False, 'message': msg or 'Incremental comparison failed.'})
            payload['comparison'] = comparison
            payload['message'] = f'Part {part_num} uploaded + compared against {baseline_source}.'

        return jsonify(payload)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/incremental_comparison_status')
def incremental_comparison_status():
    """Fetch the current accumulated comparison for an incremental session."""
    session_id = request.args.get('session_id')
    if not session_id:
        return jsonify({'success': False, 'message': 'Missing session_id'}), 400

    state = analyzer.incremental_compare_sessions.get(session_id)
    if not state:
        # Attempt to load persisted summary (if any)
        path = os.path.join(analyzer.comparison_session_dir, f'{session_id}.json')
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    payload = json.load(f)
                payload['success'] = True
                return jsonify(payload)
            except Exception as e:
                return jsonify({'success': False, 'message': f'Unable to load saved session: {str(e)}'}), 500
        return jsonify({'success': False, 'message': 'Session not found'}), 404

    payload = analyzer._incremental_state_to_payload(state)
    payload['success'] = True
    return jsonify(payload)

@app.route('/finalize_multipart', methods=['POST'])
def finalize_multipart():
    """
    After all parts are uploaded, stream them as one JSON.
    If baseline_source is provided, runs a streaming comparison without concatenating files.
    """
    try:
        session_id = request.form.get('session_id')
        source_name = request.form.get('source_name')
        baseline_source = request.form.get('baseline_source')

        if not session_id:
            return jsonify({'success': False, 'message': 'session_id is required.'})

        part_paths = analyzer.get_multipart_paths(session_id)
        if not part_paths:
            return jsonify({'success': False, 'message': 'No parts found for this session. Upload parts first.'})

        # Use stored source name if client didn't override
        if not source_name:
            source_name = analyzer.multipart_sessions.get(session_id, {}).get('source_name', f'Source_{session_id[:6]}')

        if baseline_source:
            if baseline_source not in analyzer.cpt_pricing:
                return jsonify({'success': False, 'message': f'Baseline source \"{baseline_source}\" not loaded yet.'})
            comparison, msg = analyzer.stream_compare(part_paths, baseline_source)
            if comparison:
                comparison['from_parts'] = True
                comparison['part_count'] = len(part_paths)
                comparison['success'] = True
                comparison['source1'] = source_name
                return jsonify(comparison)
            return jsonify({'success': False, 'message': msg or 'Comparison failed.'})

        # Otherwise, fully load CPT data from the combined stream
        response_payload = analyzer.load_json_from_parts(part_paths, source_name)
        return jsonify(response_payload)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/load_paginated', methods=['POST'])
def load_paginated():
    """Load large file with pagination - returns one page at a time"""
    try:
        # Check if file_id from /upload is provided (smart auto-loading)
        if request.form.get('file_id') and not request.files.get('file'):
            file_id = request.form.get('file_id')
            page = int(request.form.get('page', 1))
            page_size = int(request.form.get('page_size', 500))
            
            # Check if this is from /upload endpoint
            source_key_large = f'_large_{file_id}'
            source_key_paginated = f'_paginated_{file_id}'
            
            if source_key_large in analyzer.data_sources:
                # File uploaded via /upload, now loading paginated
                file_path = analyzer.data_sources[source_key_large]['path']
                
                # Move to paginated storage
                analyzer.data_sources[source_key_paginated] = {
                    'path': file_path,
                    'type': 'paginated_file'
                }
                
                result = analyzer.extract_cpt_pricing_paginated(file_path, page, page_size)
                result['file_id'] = file_id
                return jsonify(result)
                
            elif source_key_paginated in analyzer.data_sources:
                # Already in paginated mode
                file_path = analyzer.data_sources[source_key_paginated]['path']
                result = analyzer.extract_cpt_pricing_paginated(file_path, page, page_size)
                result['file_id'] = file_id
                return jsonify(result)
            else:
                return jsonify({'success': False, 'message': 'File session expired. Please re-upload.'})
        
        # Check if this is initial upload or pagination request
        elif 'file' in request.files and request.files['file'].filename:
            # Initial upload - save file and return first page
            file = request.files['file']
            temp_path = analyzer.save_uploaded_file(file, 'paginated')
            
            # Store path in session or return to client
            # For simplicity, we'll use a simple in-memory cache
            file_id = hashlib.sha256(temp_path.encode()).hexdigest()[:16]
            analyzer.data_sources[f'_paginated_{file_id}'] = {'path': temp_path, 'type': 'paginated_file'}
            
            page = int(request.form.get('page', 1))
            page_size = int(request.form.get('page_size', 500))
            
            result = analyzer.extract_cpt_pricing_paginated(temp_path, page, page_size)
            result['file_id'] = file_id
            return jsonify(result)
            
        elif request.form.get('file_id'):
            # Pagination request for existing file
            file_id = request.form.get('file_id')
            page = int(request.form.get('page', 1))
            page_size = int(request.form.get('page_size', 500))
            
            source_key = f'_paginated_{file_id}'
            if source_key not in analyzer.data_sources:
                return jsonify({'success': False, 'message': 'File session expired. Please re-upload.'})
            
            file_path = analyzer.data_sources[source_key]['path']
            result = analyzer.extract_cpt_pricing_paginated(file_path, page, page_size)
            result['file_id'] = file_id
            return jsonify(result)
        else:
            return jsonify({'success': False, 'message': 'No file or file_id provided'})
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/compare_paginated', methods=['POST'])
def compare_paginated():
    """Compare large file page-by-page against a loaded baseline source"""
    try:
        baseline_source = request.form.get('baseline_source')
        
        # Check if baseline file is being uploaded
        if 'baseline_file' in request.files and request.files['baseline_file'].filename:
            baseline_file = request.files['baseline_file']
            baseline_filename = baseline_file.filename.lower()
            baseline_source = request.form.get('baseline_source') or 'Source 2'
            
            # Save and load baseline file
            baseline_path = analyzer.save_uploaded_file(baseline_file, 'baseline')
            
            # Load based on file type
            if baseline_filename.endswith(('.xlsx', '.xls')):
                success, message = analyzer.load_excel_file(baseline_path, baseline_source)
                if not success:
                    return jsonify({'success': False, 'message': f'Error loading baseline: {message}'})
            elif baseline_filename.endswith('.csv'):
                success, message, _ = analyzer.load_csv_file(baseline_path, baseline_source)
                if not success:
                    return jsonify({'success': False, 'message': f'Error loading baseline: {message}'})
            else:
                return jsonify({'success': False, 'message': 'Baseline must be Excel (.xlsx, .xls) or CSV (.csv) file'})
        
        # Validate baseline is loaded
        if not baseline_source:
            return jsonify({'success': False, 'message': 'Please provide baseline_source name or upload baseline_file.'})
        
        if baseline_source not in analyzer.cpt_pricing:
            return jsonify({'success': False, 'message': f'Baseline source "{baseline_source}" not loaded. Please upload baseline_file or load it first via /upload.'})
        
        # Check if this is initial upload or pagination request
        if 'file' in request.files and request.files['file'].filename:
            # Initial upload - save file and compare first page
            file = request.files['file']
            temp_path = analyzer.save_uploaded_file(file, 'compare_paginated')
            
            # Store path
            file_id = hashlib.sha256(temp_path.encode()).hexdigest()[:16]
            analyzer.data_sources[f'_compare_{file_id}'] = {
                'path': temp_path, 
                'type': 'compare_paginated_file',
                'baseline': baseline_source
            }
            
            page = int(request.form.get('page', 1))
            page_size = int(request.form.get('page_size', 500))
            
            comparison, msg = analyzer.compare_paginated(temp_path, baseline_source, page, page_size)
            
            if comparison:
                comparison['file_id'] = file_id
                comparison['success'] = True
                return jsonify(comparison)
            else:
                return jsonify({'success': False, 'message': msg})
                
        elif request.form.get('file_id'):
            # Pagination request for existing comparison
            file_id = request.form.get('file_id')
            page = int(request.form.get('page', 1))
            page_size = int(request.form.get('page_size', 500))
            
            source_key = f'_compare_{file_id}'
            if source_key not in analyzer.data_sources:
                return jsonify({'success': False, 'message': 'Comparison session expired. Please re-upload.'})
            
            file_path = analyzer.data_sources[source_key]['path']
            baseline = analyzer.data_sources[source_key]['baseline']
            
            comparison, msg = analyzer.compare_paginated(file_path, baseline, page, page_size)
            
            if comparison:
                comparison['file_id'] = file_id
                comparison['success'] = True
                return jsonify(comparison)
            else:
                return jsonify({'success': False, 'message': msg})
        else:
            return jsonify({'success': False, 'message': 'No file or file_id provided'})
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/sources')
def get_sources():
    """Get list of loaded sources"""
    return jsonify({
        'sources': list(analyzer.data_sources.keys())
    })

@app.route('/export_source_csv')
def export_source_csv():
    """Export a loaded CPT source as CSV"""
    source_name = request.args.get('source')
    if not source_name:
        return jsonify({'success': False, 'message': 'Missing source parameter'}), 400
    if source_name not in analyzer.cpt_pricing:
        return jsonify({'success': False, 'message': f'No CPT pricing loaded for {source_name}'}), 404

    cpt_data = analyzer.cpt_pricing[source_name]
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['S.No', 'CPT Code', 'Description', 'Negotiated Rate', 'Billing Class', 'Service Codes'])

    serial = 1
    for code in sorted(cpt_data.keys()):
        info = cpt_data[code]
        if info['rates']:
            for rate in info['rates']:
                writer.writerow([
                    serial,
                    code,
                    info['description'],
                    rate.get('negotiated_rate', ''),
                    rate.get('billing_class', ''),
                    ';'.join(rate.get('service_code', []))
                ])
                serial += 1
        else:
            writer.writerow([serial, code, info['description'], '', '', ''])
            serial += 1

    response = make_response(output.getvalue())
    response.headers['Content-Disposition'] = f'attachment; filename="{source_name}_cpt_pricing.csv"'
    response.headers['Content-Type'] = 'text/csv'
    return response

@app.route('/export_comparison_csv')
def export_comparison_csv():
    """Export comparison results to CSV"""
    source1 = request.args.get('source1')
    source2 = request.args.get('source2')
    compare_rule = (request.args.get('compare_rule') or request.args.get('rule') or 'max').strip().lower()
    negotiated_type = (request.args.get('negotiated_type') or '').strip()
    exclude_expired_raw = (request.args.get('exclude_expired') or '').strip().lower()
    exclude_expired = exclude_expired_raw in ('1', 'true', 'yes', 'on')
    if not source1 or not source2:
        return jsonify({'success': False, 'message': 'Missing source1 or source2 parameter'}), 400

    comparison = analyzer.compare_pricing(
        source1,
        source2,
        compare_rule=compare_rule,
        negotiated_type=negotiated_type or None,
        exclude_expired=exclude_expired
    )
    if not comparison:
        return jsonify({'success': False, 'message': 'Comparison data not available. Please load CPT pricing for both sources first.'}), 404

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Summary Metric', 'Value'])
    writer.writerow(['Total Compared', comparison.get('total_compared', 0)])
    writer.writerow([f'Higher in {source1} (count)', len(comparison.get('higher_in_source1', []))])
    writer.writerow([f'Higher in {source1} (total)', comparison.get('total_higher_in_source1_amount', 0)])
    writer.writerow([f'Lower in {source1} (count)', len(comparison.get('higher_in_source2', []))])
    writer.writerow([f'Lower in {source1} (total)', comparison.get('total_higher_in_source2_amount', 0)])
    writer.writerow([f'Higher in {source2} (count)', len(comparison.get('higher_in_source2', []))])
    writer.writerow([f'Higher in {source2} (total)', comparison.get('total_higher_in_source2_amount', 0)])
    writer.writerow([f'Lower in {source2} (count)', len(comparison.get('higher_in_source1', []))])
    writer.writerow([f'Lower in {source2} (total)', comparison.get('total_higher_in_source1_amount', 0)])
    writer.writerow(['Equal Pricing', len(comparison.get('equal', []))])
    writer.writerow([])
    writer.writerow(['S.No', 'Bucket', 'CPT Code', 'Source 1 Description', 'Source 2 Description', f'{source1} Rate', f'{source2} Rate', 'Difference (Source1-Source2)', 'Percent Difference'])

    serial = 1

    def write_rows(items, bucket_label):
        nonlocal serial
        for item in items:
            writer.writerow([
                serial,
                bucket_label,
                item.get('code', ''),
                item.get('source1_description', item.get('description', '')),
                item.get('source2_description', ''),
                item.get('source1_rate', item.get('rate', '')),
                item.get('source2_rate', ''),
                item.get('difference', 0),
                item.get('percent_difference', 0)
            ])
            serial += 1

    write_rows(comparison.get('higher_in_source1', []), f'Higher in {source1}')
    write_rows(comparison.get('higher_in_source2', []), f'Higher in {source2}')
    write_rows(comparison.get('equal', []), 'Equal Pricing')

    for item in comparison.get('only_in_source1', []):
        writer.writerow([
            serial,
            f'Only in {source1}',
            item.get('code', ''),
            item.get('description', ''),
            '',
            item.get('rate', ''),
            '',
            '',
            ''
        ])
        serial += 1

    for item in comparison.get('only_in_source2', []):
        writer.writerow([
            serial,
            f'Only in {source2}',
            item.get('code', ''),
            '',
            item.get('description', ''),
            '',
            item.get('rate', ''),
            '',
            ''
        ])
        serial += 1

    response = make_response(output.getvalue())
    response.headers['Content-Disposition'] = f'attachment; filename="{source1}_vs_{source2}_comparison.csv"'
    response.headers['Content-Type'] = 'text/csv'
    return response

@app.route('/export_incremental_comparison_csv')
def export_incremental_comparison_csv():
    """Export incremental comparison session (summary + sample rows) to CSV."""
    session_id = request.args.get('session_id')
    if not session_id:
        return jsonify({'success': False, 'message': 'Missing session_id parameter'}), 400

    state = analyzer.incremental_compare_sessions.get(session_id)
    if state:
        comparison = analyzer._incremental_state_to_payload(state)
    else:
        path = os.path.join(analyzer.comparison_session_dir, f'{session_id}.json')
        if not os.path.exists(path):
            return jsonify({'success': False, 'message': 'Session not found'}), 404
        with open(path, 'r', encoding='utf-8') as f:
            comparison = json.load(f)

    source1 = comparison.get('source1', 'Source 1')
    source2 = comparison.get('source2', 'Source 2')

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Summary Metric', 'Value'])
    writer.writerow(['Session ID', session_id])
    writer.writerow(['Parts Processed', comparison.get('parts_processed', 0)])
    writer.writerow(['Total Compared', comparison.get('total_compared', 0)])
    writer.writerow(['Total Source1 Unique Codes', comparison.get('total_source1_count', 0)])
    writer.writerow(['Total Source2 Codes', comparison.get('total_source2', 0)])
    writer.writerow([f'Higher in {source1} (count)', comparison.get('higher_in_source1_count', len(comparison.get('higher_in_source1', [])))])
    writer.writerow([f'Higher in {source1} (total)', comparison.get('total_higher_in_source1_amount', 0)])
    writer.writerow([f'Lower in {source1} (count)', comparison.get('higher_in_source2_count', len(comparison.get('higher_in_source2', [])))])
    writer.writerow([f'Lower in {source1} (total)', comparison.get('total_higher_in_source2_amount', 0)])
    writer.writerow(['Equal Pricing (count)', comparison.get('equal_count', len(comparison.get('equal', [])))])
    writer.writerow(['Only in Source 1 (count)', comparison.get('only_in_source1_count', 0)])
    writer.writerow(['Only in Source 2 (count)', comparison.get('only_in_source2_count', 0)])
    writer.writerow([])
    writer.writerow(['Note', 'Detail rows are samples only (limited).'])
    writer.writerow([])
    writer.writerow(['S.No', 'Bucket', 'CPT Code', 'Source 1 Description', 'Source 2 Description', f'{source1} Rate', f'{source2} Rate', 'Difference (Source1-Source2)', 'Percent Difference'])

    serial = 1

    def write_rows(items, bucket_label):
        nonlocal serial
        for item in items:
            writer.writerow([
                serial,
                bucket_label,
                item.get('code', ''),
                item.get('source1_description', item.get('description', '')),
                item.get('source2_description', ''),
                item.get('source1_rate', item.get('rate', '')),
                item.get('source2_rate', ''),
                item.get('difference', 0),
                item.get('percent_difference', 0)
            ])
            serial += 1

    write_rows(comparison.get('higher_in_source1', []), f'Higher in {source1}')
    write_rows(comparison.get('higher_in_source2', []), f'Higher in {source2}')
    write_rows(comparison.get('equal', []), 'Equal Pricing')

    response = make_response(output.getvalue())
    response.headers['Content-Disposition'] = f'attachment; filename=\"{source1}_vs_{source2}_incremental_{session_id}.csv\"'
    response.headers['Content-Type'] = 'text/csv'
    return response

if __name__ == '__main__':
    app.run(debug=True, port=5001)

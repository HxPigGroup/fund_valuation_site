from __future__ import annotations

import json
import multiprocessing as mp
import os
import re
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from functools import partial
from html import escape
from io import StringIO
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import akshare as ak
import pandas as pd
import requests
from akshare.utils import demjson

try:
    import tushare as ts
except Exception:
    ts = None

BASE_DIR = Path('/root/fund_valuation_site')
ENV_FILE = BASE_DIR / '.env'
TRACKED_FILE = BASE_DIR / 'tracked_funds.txt'
CACHE_FILE = BASE_DIR / 'valuation_cache.json'
STATUS_FILE = BASE_DIR / 'refresh_status.json'
CATALOG_FILE = BASE_DIR / 'fund_catalog.json'
USER_DATA_DIR = BASE_DIR / 'user_data'
PUBLIC_PAGE_KEY = '__public__'


def _load_env_file() -> None:
    if not ENV_FILE.exists():
        return
    for raw in ENV_FILE.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_env_file()

HOST = '0.0.0.0'
PORT = 11452
AUTO_REFRESH_SECONDS = int(os.getenv('FUND_AUTO_REFRESH_SECONDS', '900'))
AUTO_REFRESH_LOOP_ENABLED = os.getenv('FUND_ENABLE_AUTO_REFRESH_LOOP', '0') == '1'
HOLDINGS_TIMEOUT_SECONDS = int(os.getenv('FUND_HOLDINGS_TIMEOUT_SECONDS', '12'))
STOCK_SPOT_TIMEOUT_SECONDS = int(os.getenv('FUND_STOCK_SPOT_TIMEOUT_SECONDS', '18'))
QUOTE_CACHE_TTL_SECONDS = int(os.getenv('FUND_QUOTE_CACHE_TTL_SECONDS', '300'))
ESTIMATION_CACHE_TTL_SECONDS = int(os.getenv('FUND_ESTIMATION_CACHE_TTL_SECONDS', '300'))
CODE_ESTIMATION_TIMEOUT_SECONDS = int(os.getenv('FUND_CODE_ESTIMATION_TIMEOUT_SECONDS', '6'))
UPSTREAM_CONNECT_TIMEOUT_SECONDS = int(os.getenv('FUND_UPSTREAM_CONNECT_TIMEOUT_SECONDS', '5'))
UPSTREAM_READ_TIMEOUT_SECONDS = int(os.getenv('FUND_UPSTREAM_READ_TIMEOUT_SECONDS', '15'))
TUSHARE_TIMEOUT_SECONDS = int(os.getenv('FUND_TUSHARE_TIMEOUT_SECONDS', '12'))
ESTIMATION_PAGE_SIZE = 20000
ESTIMATION_MAX_PAGES = 5
SELF_ESTIMATE_HISTORY_DAYS = int(os.getenv('FUND_SELF_ESTIMATE_HISTORY_DAYS', '5'))
TUSHARE_DAILY_LOOKBACK_DAYS = int(os.getenv('FUND_TUSHARE_DAILY_LOOKBACK_DAYS', '20'))
ESTIMATION_SYMBOLS = ('全部', '股票型', '混合型', '债券型', '指数型', 'QDII', 'ETF联接', 'LOF', '场内交易基金')

_refresh_lock = threading.Lock()
_refresh_queue_lock = threading.Lock()
_tushare_lock = threading.Lock()
_tushare_pro = None
_quote_cache = {'updated_at': 0.0, 'rows': {}}
_estimation_cache = {'updated_at': 0.0, 'rows': {}, 'available': False}
_refresh_queue: deque[str] = deque()
_queued_refresh_phones: set[str] = set()
_active_refresh_phone = ''
_refresh_worker_started = False


class FundHandler(SimpleHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path not in {'/', ''}:
            self.send_error(404, 'Not Found')
            return

        query = parse_qs(parsed.query)
        raw_phone = query.get('phone', [''])[0]
        phone = _normalize_phone(raw_phone)
        catalog_payload = _read_json(CATALOG_FILE, {})
        catalog = catalog_payload.get('items', []) if isinstance(catalog_payload, dict) else []

        if phone:
            _ensure_user_files(phone)
            tracked_codes = _read_tracked_codes(phone)
            cache_payload = _read_json(_cache_file(phone), {})
            cached_rows = cache_payload.get('rows', {}) if isinstance(cache_payload, dict) else {}
            refreshed_at = cache_payload.get('refreshed_at', '') if isinstance(cache_payload, dict) else ''
            status = _read_json(_status_file(phone), {})
            if tracked_codes and not cached_rows and status.get('status') != 'running':
                _trigger_refresh(phone)
                status = _read_json(_status_file(phone), {})
            html = _render_page(phone, tracked_codes, cached_rows, refreshed_at, status, catalog, raw_phone=raw_phone)
        else:
            tracked_codes = _read_tracked_codes()
            cache_payload = _read_json(CACHE_FILE, {})
            cached_rows = cache_payload.get('rows', {}) if isinstance(cache_payload, dict) else {}
            refreshed_at = cache_payload.get('refreshed_at', '') if isinstance(cache_payload, dict) else ''
            status = _read_json(STATUS_FILE, {})
            if tracked_codes and not cached_rows and status.get('status') != 'running':
                _trigger_refresh()
                status = _read_json(STATUS_FILE, {})
            html = _render_page('', tracked_codes, cached_rows, refreshed_at, status, catalog, is_public=True, raw_phone=raw_phone)

        encoded = html.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(encoded)))
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        self.end_headers()
        self.wfile.write(encoded)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        content_length = int(self.headers.get('Content-Length', '0'))
        raw = self.rfile.read(content_length).decode('utf-8')
        form = parse_qs(raw)
        phone = _normalize_phone(form.get('phone', [''])[0])
        code = _normalize_code(form.get('code', [''])[0])

        if parsed.path == '/refresh':
            if phone:
                _ensure_user_files(phone)
                _trigger_refresh(phone)
                return self._redirect_home(phone)
            _trigger_refresh()
            return self._redirect_home()

        if parsed.path == '/tracked/add' and code:
            if phone:
                _ensure_user_files(phone)
                tracked = _read_tracked_codes(phone)
                if code not in tracked:
                    tracked.append(code)
                    _write_tracked_codes(tracked, phone)
                _trigger_refresh(phone)
                return self._redirect_home(phone)
            tracked = _read_tracked_codes()
            if code not in tracked:
                tracked.append(code)
                _write_tracked_codes(tracked)
            _trigger_refresh()
            return self._redirect_home()

        if parsed.path == '/tracked/delete' and code:
            if phone:
                _ensure_user_files(phone)
                tracked = [item for item in _read_tracked_codes(phone) if item != code]
                _write_tracked_codes(tracked, phone)
                _trigger_refresh(phone)
                return self._redirect_home(phone)
            tracked = [item for item in _read_tracked_codes() if item != code]
            _write_tracked_codes(tracked)
            _trigger_refresh()
            return self._redirect_home()

        self.send_error(400, 'Unsupported request')

    def _redirect_home(self, phone: str = '') -> None:
        self.send_response(303)
        if phone:
            self.send_header('Location', f'/?phone={phone}')
        else:
            self.send_header('Location', '/')
        self.end_headers()


def main() -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
    TRACKED_FILE.touch(exist_ok=True)
    if not CACHE_FILE.exists():
        CACHE_FILE.write_text(json.dumps({'rows': {}, 'refreshed_at': ''}, ensure_ascii=False, indent=2), encoding='utf-8')
    if not STATUS_FILE.exists():
        _write_status('idle', '等待刷新')
    if not CATALOG_FILE.exists():
        CATALOG_FILE.write_text(json.dumps({'items': []}, ensure_ascii=False, indent=2), encoding='utf-8')

    _start_refresh_worker()
    _reset_stale_status('')
    if _read_tracked_codes() and _cache_needs_refresh(CACHE_FILE):
        _trigger_refresh()
    for phone in _list_all_user_phones():
        _ensure_user_files(phone)
        _reset_stale_status(phone)
        if _read_tracked_codes(phone) and _cache_needs_refresh(_cache_file(phone)):
            _trigger_refresh(phone)
    if AUTO_REFRESH_LOOP_ENABLED:
        _start_auto_refresh_loop()

    handler = partial(FundHandler, directory=str(BASE_DIR))
    server = ThreadingHTTPServer((HOST, PORT), handler)
    server.serve_forever()


def _trigger_refresh(phone: str = '') -> None:
    global _active_refresh_phone

    normalized_phone = _normalize_phone(phone)
    scope_key = normalized_phone or PUBLIC_PAGE_KEY
    if normalized_phone:
        _ensure_user_files(normalized_phone)

    with _refresh_queue_lock:
        if scope_key == _active_refresh_phone or scope_key in _queued_refresh_phones:
            _write_status('running', '已有基金估值刷新任务在执行', phone=normalized_phone)
            return
        _refresh_queue.append(scope_key)
        _queued_refresh_phones.add(scope_key)

    _write_status('running', '已加入刷新队列', phone=normalized_phone)


def _start_refresh_worker() -> None:
    global _refresh_worker_started

    if _refresh_worker_started:
        return
    _refresh_worker_started = True
    threading.Thread(target=_refresh_worker_loop, daemon=True).start()


def _refresh_worker_loop() -> None:
    global _active_refresh_phone

    while True:
        scope_key = ''
        with _refresh_queue_lock:
            if _refresh_queue:
                scope_key = _refresh_queue.popleft()
                _queued_refresh_phones.discard(scope_key)
                _active_refresh_phone = scope_key
        if not scope_key:
            time.sleep(0.5)
            continue

        phone = '' if scope_key == PUBLIC_PAGE_KEY else scope_key
        with _refresh_lock:
            try:
                _write_status('running', '正在刷新基金估值', phone=phone)
                rows = _fetch_tracked_rows_with_timeout(_read_tracked_codes(phone))
                _cache_file(phone).write_text(
                    json.dumps({'rows': rows, 'refreshed_at': _now()}, ensure_ascii=False, indent=2),
                    encoding='utf-8',
                )
                official_count = sum(
                    1 for row in rows.values()
                    if _as_float(row.get('estimate_growth')) is not None
                )
                message = f'已刷新 {len(rows)} 只基金'
                if rows and official_count == 0:
                    message += '，官方估值接口暂无数据'
                _write_status('success', message, _now(), phone=phone)
            except Exception as exc:
                _log(f'刷新失败 scope={phone or "public"}: {exc}')
                _write_status('error', str(exc), phone=phone)
            finally:
                with _refresh_queue_lock:
                    if _active_refresh_phone == scope_key:
                        _active_refresh_phone = ''


def _fetch_tracked_rows_with_timeout(tracked_codes: list[str]) -> dict[str, dict]:
    ctx = mp.get_context('fork')
    result_queue: mp.Queue = ctx.Queue()
    process = ctx.Process(target=_tracked_rows_worker, args=(tracked_codes, result_queue))
    process.start()
    process.join(240)
    if process.is_alive():
        process.terminate()
        process.join(1)
        raise TimeoutError('基金估值刷新超过 240 秒，已主动终止')
    try:
        status, result = result_queue.get(timeout=2)
    except Exception as exc:
        raise RuntimeError('刷新子进程没有返回结果') from exc
    if status != 'success':
        raise RuntimeError(str(result))
    return result


def _tracked_rows_worker(tracked_codes: list[str], result_queue: mp.Queue) -> None:
    try:
        result_queue.put(('success', _fetch_tracked_rows(tracked_codes)))
    except Exception as exc:
        result_queue.put(('error', str(exc)))


def _fetch_tracked_rows(tracked_codes: list[str]) -> dict[str, dict]:
    if not tracked_codes:
        return {}

    estimate_map = _fetch_estimation_map_cached()
    catalog_items = _read_json(CATALOG_FILE, {}).get('items', [])
    catalog_name_map = {str(item.get('code', '')).zfill(6): str(item.get('name', '')).strip() for item in catalog_items}
    rows: dict[str, dict] = {}

    for code in tracked_codes:
        nav_item = _fetch_nav_info(code)
        estimate_item = estimate_map.get(code, {})
        if estimate_item.get('estimate_value') is None and _estimation_cache.get('available', True):
            code_estimate = _fetch_estimation_by_code(code)
            if code_estimate.get('estimate_value') is not None:
                estimate_item = code_estimate
        has_official_estimate = estimate_item.get('estimate_value') is not None
        self_estimate = None
        holdings_source = 'none'
        quote_source = 'none'
        if not has_official_estimate:
            holdings, holdings_source = _fetch_holdings(code)
            quotes, quote_source = _fetch_stock_quotes([item['stock_code'] for item in holdings[:10]])
            self_estimate = _estimate_by_holdings(nav_item, holdings, quotes)
        name = nav_item.get('name') or estimate_item.get('name') or catalog_name_map.get(code) or code
        source_parts = []
        if has_official_estimate:
            source_parts.append('official')
        if self_estimate:
            source_parts.append(f'self:{holdings_source}+{quote_source}')
        if not source_parts:
            source_parts.append('published_only')

        rows[code] = {
            'code': code,
            'name': str(name).strip(),
            'estimate_value': _format_value(estimate_item.get('estimate_value')),
            'estimate_growth': _format_percent(estimate_item.get('estimate_growth')),
            'published_nav': _format_value(nav_item.get('published_nav')),
            'published_growth': _format_percent(nav_item.get('published_growth')),
            'month_growth': _format_percent(nav_item.get('month_growth')),
            'deviation': _format_percent(estimate_item.get('deviation')),
            'self_estimate_value': _format_value(self_estimate.get('value') if self_estimate else None),
            'self_estimate_growth': _format_percent(self_estimate.get('growth') if self_estimate else None),
            'holdings_coverage': _format_percent(self_estimate.get('coverage') if self_estimate else None),
            'holding_count': int(self_estimate.get('holding_count') or 0) if self_estimate else 0,
            'estimate_source': ' | '.join(source_parts),
        }

    _maybe_refresh_catalog(rows)
    return rows


def _fetch_nav_info(code: str) -> dict[str, object]:
    name = _read_catalog_name(code)
    try:
        url = 'https://api.fund.eastmoney.com/f10/lsjz'
        response = requests.get(
            url,
            params={'fundCode': code, 'pageIndex': 1, 'pageSize': 30},
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://fundf10.eastmoney.com/',
            },
            timeout=(UPSTREAM_CONNECT_TIMEOUT_SECONDS, UPSTREAM_READ_TIMEOUT_SECONDS),
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get('Data') if isinstance(payload, dict) else None
        raw_records = data.get('LSJZList', []) if isinstance(data, dict) else []
        if raw_records:
            records = list(reversed(raw_records))
            latest = records[-1]
            nav_history = [
                {'date': str(item.get('FSRQ') or '').strip(), 'nav': _as_float(item.get('DWJZ'))}
                for item in records
                if _as_float(item.get('DWJZ')) is not None
            ]
            return {
                'name': name,
                'published_nav': _as_float(latest.get('DWJZ')),
                'published_growth': _as_float(latest.get('JZZZL')),
                'month_growth': _calc_recent_growth(records, 'DWJZ', 'FSRQ'),
                'nav_history': nav_history[-12:],
            }
    except (requests.RequestException, ValueError, TypeError, AttributeError):
        pass

    return _fetch_nav_info_tushare(code, name)


def _fetch_nav_info_tushare(code: str, name: str) -> dict[str, object]:
    ctx = mp.get_context('fork')
    queue: mp.Queue = ctx.Queue()
    process = ctx.Process(target=_nav_worker_tushare, args=(code, name, queue))
    process.start()
    process.join(TUSHARE_TIMEOUT_SECONDS)
    if process.is_alive():
        process.terminate()
        process.join(1)
        return {'name': name, 'published_nav': None, 'published_growth': None, 'month_growth': None}
    try:
        result = queue.get_nowait()
    except Exception:
        result = None
    return result or {'name': name, 'published_nav': None, 'published_growth': None, 'month_growth': None}


def _nav_worker_tushare(code: str, name: str, queue: mp.Queue) -> None:
    try:
        pro = _get_tushare_pro()
        if not pro:
            queue.put(None)
            return
        for ts_code in _candidate_fund_ts_codes(code):
            try:
                df = pro.fund_nav(ts_code=ts_code, limit=25)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            if 'nav_date' in df.columns:
                df = df.sort_values('nav_date')
            records = df.to_dict(orient='records')
            row = records[-1]
            nav_history = [
                {'date': str(item.get('nav_date') or '').strip(), 'nav': _as_float(item.get('unit_nav'))}
                for item in records
                if _as_float(item.get('unit_nav')) is not None
            ]
            queue.put({
                'name': name,
                'published_nav': _as_float(row.get('unit_nav')),
                'published_growth': _calc_pct(row.get('unit_nav'), row.get('pre_unit_nav')),
                'month_growth': _calc_recent_growth(records, 'unit_nav', 'nav_date'),
                'nav_history': nav_history[-12:],
            })
            return
        queue.put(None)
    except Exception:
        queue.put(None)


def _fetch_estimation_map_cached() -> dict[str, dict]:
    now = time.time()
    if now - float(_estimation_cache.get('updated_at', 0.0)) < ESTIMATION_CACHE_TTL_SECONDS:
        return dict(_estimation_cache.get('rows', {}))

    estimate_map: dict[str, dict] = {}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://fund.eastmoney.com/',
    }
    for page in range(1, ESTIMATION_MAX_PAGES + 1):
        try:
            response = requests.get(
                'https://api.fund.eastmoney.com/FundGuZhi/GetFundGZList',
                params={
                    'type': 1,
                    'sort': 3,
                    'orderType': 'desc',
                    'canbuy': 0,
                    'pageIndex': page,
                    'pageSize': ESTIMATION_PAGE_SIZE,
                    '_': int(time.time() * 1000),
                },
                headers=headers,
                timeout=(UPSTREAM_CONNECT_TIMEOUT_SECONDS, UPSTREAM_READ_TIMEOUT_SECONDS),
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError, TypeError) as exc:
            _log(f'官方估值接口请求失败 page={page}: {exc}')
            break

        data = payload.get('Data') if isinstance(payload, dict) else None
        raw_items = data.get('list', []) if isinstance(data, dict) else []
        if not raw_items:
            err_code = payload.get('ErrCode') if isinstance(payload, dict) else ''
            err_msg = payload.get('ErrMsg') if isinstance(payload, dict) else '响应格式异常'
            _log(f'官方估值接口无数据 page={page} err={err_code} message={err_msg}')
            break

        for record in raw_items:
            code = _normalize_code(str(record.get('bzdm', '')))
            if not code:
                continue
            item = {
                'name': str(record.get('jjjc') or '').strip(),
                'estimate_value': _as_float(record.get('gsz')),
                'estimate_growth': _as_float(record.get('gszzl')),
                'deviation': _as_float(record.get('gspc')),
                'source': 'eastmoney:list',
            }
            existing = estimate_map.get(code, {})
            if existing.get('estimate_value') is None and item.get('estimate_value') is not None:
                estimate_map[code] = item
            elif code not in estimate_map:
                estimate_map[code] = item

        if len(raw_items) < ESTIMATION_PAGE_SIZE:
            break
    _estimation_cache['updated_at'] = now
    _estimation_cache['rows'] = estimate_map
    _estimation_cache['available'] = bool(estimate_map)
    return dict(estimate_map)


def _merge_estimation_frame(estimate_map: dict[str, dict], frame, symbol: str) -> None:
    if frame is None or frame.empty:
        return
    columns = frame.columns.tolist()
    code_col = _pick_column(columns, '基金代码')
    if not code_col:
        return
    name_col = _pick_column(columns, '基金名称') or _pick_column(columns, '基金简称')
    estimate_value_col = _pick_column(columns, '估算值')
    estimate_growth_col = _pick_column(columns, '估算增长率')
    deviation_col = _pick_column(columns, '估算偏差')
    normalized = frame.copy()
    normalized[code_col] = normalized[code_col].astype(str).str.zfill(6)
    for record in normalized.to_dict(orient='records'):
        code = str(record.get(code_col, '')).zfill(6)
        if not code:
            continue
        item = {
            'name': str(record.get(name_col, '') if name_col else '').strip(),
            'estimate_value': _as_float(record.get(estimate_value_col)) if estimate_value_col else None,
            'estimate_growth': _as_float(record.get(estimate_growth_col)) if estimate_growth_col else None,
            'deviation': _as_float(record.get(deviation_col)) if deviation_col else None,
            'source': f'eastmoney:{symbol}',
        }
        existing = estimate_map.get(code, {})
        if existing.get('estimate_value') is not None:
            continue
        if item.get('estimate_value') is not None:
            estimate_map[code] = item
        elif code not in estimate_map:
            estimate_map[code] = item


def _fetch_estimation_by_code(code: str) -> dict:
    url = f'https://fundgz.1234567.com.cn/js/{code}.js?rt={int(time.time() * 1000)}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://fund.eastmoney.com/',
    }
    for _ in range(2):
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=(UPSTREAM_CONNECT_TIMEOUT_SECONDS, CODE_ESTIMATION_TIMEOUT_SECONDS),
                allow_redirects=False,
            )
            if response.status_code != 200:
                continue
            text = response.text.strip()
        except Exception:
            continue
        match = re.search(r'jsonpgz\((.*)\);?$', text)
        if not match:
            continue
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        return {
            'name': str(payload.get('name') or '').strip(),
            'estimate_value': _as_float(payload.get('gsz')),
            'estimate_growth': _as_float(payload.get('gszzl')),
            'deviation': None,
            'source': 'eastmoney:code',
        }
    return {}


def _fetch_holdings(code: str) -> tuple[list[dict], str]:
    items = _fetch_holdings_akshare(code)
    if items:
        return items, 'akshare'
    items = _fetch_holdings_tushare(code)
    if items:
        return items, 'tushare'
    return [], 'none'


def _fetch_holdings_akshare(code: str) -> list[dict]:
    ctx = mp.get_context('fork')
    queue: mp.Queue = ctx.Queue()
    process = ctx.Process(target=_holdings_worker_akshare, args=(code, queue))
    process.start()
    process.join(HOLDINGS_TIMEOUT_SECONDS)
    if process.is_alive():
        process.terminate()
        process.join(1)
        return []
    try:
        return queue.get_nowait()
    except Exception:
        return []


def _holdings_worker_akshare(code: str, queue: mp.Queue) -> None:
    try:
        years = [str(datetime.now().year), str(datetime.now().year - 1), str(datetime.now().year - 2)]
        for year in years:
            try:
                items = _fetch_holdings_year(code, year)
            except Exception:
                continue
            if items:
                queue.put(items)
                return
        queue.put([])
    except Exception:
        queue.put([])


def _fetch_holdings_year(code: str, year: str) -> list[dict]:
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
    page_url = f'https://fundf10.eastmoney.com/ccmx_{code}.html'
    session.get(
        page_url,
        timeout=(UPSTREAM_CONNECT_TIMEOUT_SECONDS, UPSTREAM_READ_TIMEOUT_SECONDS),
        proxies={},
    )
    response = session.get(
        'https://fundf10.eastmoney.com/FundArchivesDatas.aspx',
        params={
            'type': 'jjcc',
            'code': code,
            'topline': '10000',
            'year': year,
            'month': '',
            'rt': '0.913877030254846',
        },
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': page_url,
        },
        timeout=(UPSTREAM_CONNECT_TIMEOUT_SECONDS, UPSTREAM_READ_TIMEOUT_SECONDS),
        proxies={},
    )
    if response.status_code != 200:
        return []
    text = response.text.strip()
    if not text or '{' not in text:
        return []
    payload = demjson.decode(text[text.find('{'):-1])
    content = str(payload.get('content') or '').strip()
    if not content:
        return []
    try:
        tables = pd.read_html(StringIO(content), converters={'股票代码': str})
    except Exception:
        return []
    if not tables:
        return []
    frame = tables[0]
    if frame is None or frame.empty:
        return []
    if '相关资讯' in frame.columns:
        frame = frame.drop(columns=['相关资讯'])
    rename_map = {
        '占净值 比例': '占净值比例',
        '持股数（万股）': '持股数',
        '持股数 （万股）': '持股数',
        '持仓市值（万元）': '持仓市值',
        '持仓市值 （万元）': '持仓市值',
        '持仓市值（万元人民币）': '持仓市值',
        '持仓市值 （万元人民币）': '持仓市值',
    }
    frame = frame.rename(columns=rename_map)
    code_col = _pick_column(frame.columns.tolist(), '股票代码') or _pick_column(frame.columns.tolist(), '代码')
    weight_col = _pick_column(frame.columns.tolist(), '占净值比例') or _pick_column(frame.columns.tolist(), '占净值比')
    name_col = _pick_column(frame.columns.tolist(), '股票名称') or _pick_column(frame.columns.tolist(), '名称')
    if not code_col or not weight_col:
        return []
    items = []
    for record in frame.to_dict(orient='records'):
        stock_code = _normalize_code(str(record.get(code_col, '')))
        weight = _as_float(record.get(weight_col))
        if not stock_code or weight is None or weight <= 0:
            continue
        items.append({
            'stock_code': stock_code,
            'stock_name': str(record.get(name_col, '') if name_col else '').strip(),
            'weight': weight,
        })
    items.sort(key=lambda item: float(item['weight']), reverse=True)
    return items


def _fetch_holdings_tushare(code: str) -> list[dict]:
    pro = _get_tushare_pro()
    if not pro:
        return []
    best = []
    for ts_code in _candidate_fund_ts_codes(code):
        try:
            df = pro.fund_portfolio(ts_code=ts_code)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        records = df.to_dict(orient='records')
        latest_end = str(records[0].get('end_date', ''))
        items = []
        for record in records:
            if latest_end and str(record.get('end_date', '')) != latest_end:
                continue
            stock_code = _normalize_code(str(record.get('symbol', '') or record.get('stk_code', '')))
            weight = _as_float(record.get('stk_mkv_ratio') or record.get('mkv_ratio'))
            if not stock_code or weight is None or weight <= 0:
                continue
            items.append({'stock_code': stock_code, 'stock_name': str(record.get('symbol', '')).strip(), 'weight': weight})
        if items:
            items.sort(key=lambda item: float(item['weight']), reverse=True)
            best = items
            break
    return best


def _fetch_stock_quotes(codes: list[str]) -> tuple[dict[str, dict], str]:
    needed = sorted({code for code in codes if code})
    if not needed:
        return {}, 'none'

    now = time.time()
    cached_rows = dict(_quote_cache.get('rows', {}))
    if now - float(_quote_cache.get('updated_at', 0.0)) < QUOTE_CACHE_TTL_SECONDS:
        subset = {code: cached_rows[code] for code in needed if code in cached_rows}
        if subset:
            return subset, 'cache'

    quotes = _fetch_stock_quotes_tushare(needed)
    if quotes:
        _quote_cache['updated_at'] = now
        _quote_cache['rows'] = {**cached_rows, **quotes}
        return quotes, 'tushare_daily'

    quotes = _fetch_stock_quotes_akshare(needed)
    if quotes:
        _quote_cache['updated_at'] = now
        _quote_cache['rows'] = {**cached_rows, **quotes}
        return quotes, 'akshare'

    subset = {code: cached_rows[code] for code in needed if code in cached_rows}
    if subset:
        return subset, 'stale_cache'
    return {}, 'none'


def _fetch_stock_quotes_akshare(codes: list[str]) -> dict[str, dict]:
    ctx = mp.get_context('fork')
    queue: mp.Queue = ctx.Queue()
    process = ctx.Process(target=_stock_quotes_worker_akshare, args=(codes, queue))
    process.start()
    process.join(STOCK_SPOT_TIMEOUT_SECONDS)
    if process.is_alive():
        process.terminate()
        process.join(1)
        return {}
    try:
        return queue.get_nowait()
    except Exception:
        return {}


def _stock_quotes_worker_akshare(codes: list[str], queue: mp.Queue) -> None:
    try:
        frame = ak.stock_zh_a_spot_em()
        if frame is None or frame.empty:
            queue.put({})
            return
        columns = frame.columns.tolist()
        code_col = _pick_column(columns, '代码')
        name_col = _pick_column(columns, '名称')
        change_col = _pick_column(columns, '涨跌幅')
        normalized = frame.copy()
        normalized[code_col] = normalized[code_col].astype(str).str.zfill(6)
        subset = normalized[normalized[code_col].isin(set(codes))]
        rows = {}
        for record in subset.to_dict(orient='records'):
            code = str(record.get(code_col, '')).zfill(6)
            rows[code] = {'name': str(record.get(name_col, '') if name_col else '').strip(), 'pct_change': _as_float(record.get(change_col))}
        queue.put(rows)
    except Exception:
        queue.put({})


def _fetch_stock_quotes_tushare(codes: list[str]) -> dict[str, dict]:
    if ts is None or not codes:
        return {}
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=TUSHARE_DAILY_LOOKBACK_DAYS)).strftime('%Y%m%d')
    rows = {}
    for code in codes:
        item = _fetch_stock_daily_tushare(code, start_date, end_date)
        if item:
            rows[code] = item
    return rows


def _fetch_stock_daily_tushare(code: str, start_date: str, end_date: str) -> dict:
    pro = _get_tushare_pro()
    if not pro:
        return {}
    for ts_code in _candidate_ts_codes(code):
        try:
            df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        if 'trade_date' in df.columns:
            df = df.sort_values('trade_date')
        records = df.to_dict(orient='records')
        latest = records[-1]
        pct_change = _as_float(latest.get('pct_chg') or latest.get('PCT_CHG'))
        if pct_change is None:
            pct_change = _calc_pct(latest.get('close'), latest.get('pre_close'))
        window_pct_change = None
        window_span = min(SELF_ESTIMATE_HISTORY_DAYS, len(records) - 1)
        if window_span > 0:
            start_record = records[-(window_span + 1)]
            window_pct_change = _calc_pct(latest.get('close'), start_record.get('close'))
        return {
            'name': '',
            'pct_change': pct_change,
            'window_pct_change': window_pct_change,
            'trade_date': str(latest.get('trade_date') or ''),
            'source': 'tushare_daily',
        }
    return {}


def _estimate_by_holdings(nav_item: dict, holdings: list[dict], quotes: dict[str, dict]) -> dict | None:
    published_nav = _as_float(nav_item.get('published_nav'))
    if published_nav is None or not holdings or not quotes:
        return None
    nav_history = nav_item.get('nav_history') or []
    current_weighted_return = 0.0
    historical_weighted_return = 0.0
    covered_weight = 0.0
    holding_count = 0
    historical_count = 0
    seen: set[str] = set()
    for item in holdings[:10]:
        stock_code = item['stock_code']
        if stock_code in seen:
            continue
        seen.add(stock_code)
        quote = quotes.get(stock_code)
        if not quote:
            continue
        pct_change = _as_float(quote.get('pct_change'))
        window_pct_change = _as_float(quote.get('window_pct_change'))
        weight = _as_float(item.get('weight'))
        if pct_change is None or weight is None or weight <= 0:
            continue
        current_weighted_return += (weight / 100.0) * (pct_change / 100.0)
        covered_weight += weight
        holding_count += 1
        if window_pct_change is not None:
            historical_weighted_return += (weight / 100.0) * (window_pct_change / 100.0)
            historical_count += 1
    if holding_count == 0 or covered_weight <= 0:
        return None
    scale = 1.0
    fund_window_return = _history_window_return(nav_history, SELF_ESTIMATE_HISTORY_DAYS)
    if (
        fund_window_return is not None
        and historical_count > 0
        and abs(historical_weighted_return) > 1e-6
    ):
        raw_scale = (fund_window_return / 100.0) / historical_weighted_return
        scale = _clamp(raw_scale, 0.3, 1.7)
        confidence = min(1.0, covered_weight / 100.0)
        scale = 1.0 + (scale - 1.0) * confidence
    return {
        'value': published_nav * (1.0 + current_weighted_return * scale),
        'growth': current_weighted_return * scale * 100.0,
        'coverage': covered_weight,
        'holding_count': holding_count,
    }


def _get_tushare_pro():
    global _tushare_pro
    if ts is None:
        return None
    token = os.getenv('TUSHARE_TOKEN', '').strip()
    if not token:
        return None
    with _tushare_lock:
        if _tushare_pro is None:
            ts.set_token(token)
            _tushare_pro = ts.pro_api(token)
    return _tushare_pro


def _candidate_ts_codes(code: str) -> list[str]:
    if code.startswith(('5', '6', '9')):
        return [f'{code}.SH', f'{code}.SZ']
    return [f'{code}.SZ', f'{code}.SH']


def _candidate_fund_ts_codes(code: str) -> list[str]:
    candidates = [f'{code}.OF', *_candidate_ts_codes(code)]
    return list(dict.fromkeys(candidates))


def _render_phone_entry_page(raw_phone: str = '', show_error: bool = False) -> str:
    error_text = "<p class='status error'>请输入 11 位手机号后再进入个人页面。</p>" if show_error else ''
    return f"""<!doctype html>
<html lang='zh-CN'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>进入个人基金页</title>
  <style>
    :root {{ --bg:#f5f7fb; --ink:#101828; --muted:#475467; --line:#d0d5dd; --card:#ffffff; --brand:#1570ef; --danger:#d92d20; }}
    body {{ margin:0; font-family:"IBM Plex Sans","PingFang SC","Microsoft YaHei",sans-serif; background:linear-gradient(180deg,#f8fbff 0%,var(--bg) 100%); color:var(--ink); }}
    main {{ max-width:820px; margin:0 auto; padding:48px 20px 72px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:18px; padding:22px; box-shadow:0 12px 28px rgba(16,24,40,.06); }}
    .sub {{ color:var(--muted); line-height:1.8; }}
    .status.error {{ color:var(--danger); margin:12px 0; }}
    form.inline {{ display:flex; gap:12px; flex-wrap:wrap; align-items:center; margin:20px 0 12px; }}
    input[type=text] {{ border:1px solid var(--line); border-radius:12px; padding:12px 14px; min-width:280px; font-size:15px; }}
    button {{ border:none; border-radius:12px; padding:12px 18px; background:var(--brand); color:white; cursor:pointer; }}
  </style>
</head>
<body>
  <main>
    <section class='card'>
      <h1>按手机号进入个人基金跟踪页</h1>
      <p class='sub'>输入手机号后会打开专属页面。每个手机号有独立的基金列表、刷新状态和缓存结果，互不影响；新手机号首次进入默认是空白列表。</p>
      <form class='inline' method='get' action='/'>
        <input type='text' name='phone' value='{escape(raw_phone)}' inputmode='numeric' placeholder='输入 11 位手机号' required>
        <button type='submit'>进入我的页面</button>
      </form>
      {error_text}
      <p class='sub'>当前实现里，手机号只作为页面标识，不做短信验证码校验。</p>
    </section>
  </main>
</body>
</html>"""


def _render_page(
    phone: str,
    tracked_codes: list[str],
    cached_rows: dict[str, dict],
    refreshed_at: str,
    status: dict,
    catalog: list[dict],
    is_public: bool = False,
    raw_phone: str = '',
) -> str:
    if status.get('status') == 'running':
        status_text = f"<p class='status'>基金估值刷新中，最近触发时间 {escape(str(status.get('updated_at', '')))}。</p>"
    elif status.get('status') == 'success':
        status_text = f"<p class='status'>最近一次刷新成功：{escape(str(status.get('message', '')))}。 完成时间 {escape(str(status.get('updated_at', '')))}。</p>"
    elif status.get('status') == 'error':
        status_text = f"<p class='status error'>刷新失败：{escape(str(status.get('message', '未知错误')))}</p>"
    else:
        status_text = ''
    if is_public and raw_phone and not _normalize_phone(raw_phone):
        status_text += "<p class='status error'>请输入 11 位手机号后再进入个人页面。</p>"
    if not tracked_codes and is_public:
        status_text += "<p class='sub'>公共页面暂时还没有跟踪基金，可以直接在下面添加。</p>"
    elif not tracked_codes:
        status_text += "<p class='sub'>这是一个空白个人页，先输入基金代码加入跟踪，后续这个手机号只会看到自己的列表。</p>"

    refresh_disabled = 'disabled' if status.get('status') == 'running' else ''
    refresh_label = '刷新中...' if status.get('status') == 'running' else '立即刷新估值'
    catalog_options = '\n'.join(
        f"<option value=\"{escape(str(item.get('code', '')))}\">{escape(str(item.get('code', '')))} {escape(str(item.get('name', '')))}</option>"
        for item in catalog[:800]
    )
    delete_phone_hidden_input = f"<input type='hidden' name='phone' value='{escape(phone)}'>" if phone else ''
    sorted_codes = _sort_codes_by_estimate_growth(tracked_codes, cached_rows)
    rows_html = []
    for code in sorted_codes:
        row = cached_rows.get(code, {})
        estimate_growth_class = _tone_class(row.get('estimate_growth'))
        self_estimate_growth_class = _tone_class(row.get('self_estimate_growth'))
        published_growth_class = _tone_class(row.get('published_growth'))
        month_growth_class = _tone_class(row.get('month_growth'))
        estimate_growth_cell_class = f"primary-col {estimate_growth_class}".strip()
        estimate_value_text = _blank_if_empty(row.get('estimate_value'))
        estimate_growth_text = _blank_if_empty(row.get('estimate_growth'))
        estimate_growth_html = (
            escape(estimate_growth_text)
            if estimate_growth_text
            else "<span class='empty-hint'>暂无官方估算，点“显示扩展列”看自算</span>"
        )
        rows_html.append(
            f"""
            <tr>
              <td>{escape(code)}</td>
              <td>{escape(str(row.get('name', '---')))}</td>
              <td>{escape(estimate_value_text)}</td>
              <td class='{estimate_growth_cell_class}'>{estimate_growth_html}</td>
              <td class='optional-col'>{escape(str(row.get('self_estimate_value', '---')))}</td>
              <td class='optional-col {self_estimate_growth_class}'>{escape(str(row.get('self_estimate_growth', '---')))}</td>
              <td class='{published_growth_class}'>{escape(str(row.get('published_growth', '---')))}</td>
              <td class='{month_growth_class}'>{escape(str(row.get('month_growth', '---')))}</td>
              <td>
                <form method='post' action='/tracked/delete'>
                  {delete_phone_hidden_input}
                  <input type='hidden' name='code' value='{escape(code)}'>
                  <button type='submit' class='danger'>删除</button>
                </form>
              </td>
            </tr>
            """
        )
    table_html = '\n'.join(rows_html) or "<tr><td colspan='9'>当前没有跟踪的基金。</td></tr>"
    masked_phone = _mask_phone(phone)
    page_desc = (
        f"个人页：{escape(masked_phone)} | 页面标识：{escape(phone)} | 最近缓存时间: {escape(refreshed_at or '暂无')} | 后台自动刷新: {'开启' if AUTO_REFRESH_LOOP_ENABLED else '关闭'}"
        if phone
        else f"公共页面 | 最近缓存时间: {escape(refreshed_at or '暂无')} | 后台自动刷新: {'开启' if AUTO_REFRESH_LOOP_ENABLED else '关闭'}"
    )
    page_tools = (
        f"""
      <form class='inline' method='get' action='/'>
        <input type='text' name='phone' value='{escape(phone)}' inputmode='numeric' placeholder='输入 11 位手机号' required>
        <button type='submit'>切换个人页面</button>
      </form>
      <form class='inline' method='get' action='/'><button type='submit'>回到公共页面</button></form>
        """
        if phone
        else f"""
      <form class='inline' method='get' action='/'>
        <input type='text' name='phone' value='{escape(raw_phone)}' inputmode='numeric' placeholder='输入 11 位手机号进入个人页面'>
        <button type='submit'>进入个人页面</button>
      </form>
        """
    )
    phone_hidden_input = f"<input type='hidden' name='phone' value='{escape(phone)}'>" if phone else ''
    extra_columns_toggle = "<button type='button' id='toggle-extra-columns' class='secondary' onclick='toggleExtraColumns()'>显示扩展列</button>"
    return f"""<!doctype html>
<html lang='zh-CN'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>基金估值跟踪</title>
  <style>
    :root {{ --bg:#f5f7fb; --ink:#101828; --muted:#475467; --line:#d0d5dd; --card:#ffffff; --brand:#1570ef; --danger:#d92d20; }}
    body {{ margin:0; font-family:"IBM Plex Sans","PingFang SC","Microsoft YaHei",sans-serif; background:linear-gradient(180deg,#f8fbff 0%,var(--bg) 100%); color:var(--ink); }}
    main {{ max-width:1280px; margin:0 auto; padding:36px 20px 72px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:18px; padding:18px; box-shadow:0 12px 28px rgba(16,24,40,.06); margin-bottom:18px; }}
    .sub {{ color:var(--muted); line-height:1.7; }}
    form.inline {{ display:flex; gap:12px; flex-wrap:wrap; align-items:center; margin-bottom:12px; }}
    input[type=text] {{ border:1px solid var(--line); border-radius:12px; padding:12px 14px; min-width:240px; font-size:14px; }}
    button {{ border:none; border-radius:12px; padding:10px 16px; background:var(--brand); color:white; cursor:pointer; }}
    button:disabled {{ opacity:.6; cursor:wait; }}
    button.danger {{ background:var(--danger); }}
    button.secondary {{ background:#eaf2ff; color:#175cd3; }}
    table {{ width:100%; border-collapse:collapse; }}
    th,td {{ border-bottom:1px solid #eaecf0; padding:10px 8px; text-align:left; font-size:14px; white-space:nowrap; }}
    th {{ color:var(--muted); }}
    th.primary-col, td.primary-col {{ font-weight:700; }}
    td.up {{ color:#d92d20; font-weight:600; }}
    td.down {{ color:#027a48; font-weight:600; }}
    .empty-hint {{ display:inline-block; max-width:160px; color:var(--muted); font-size:12px; font-weight:500; line-height:1.45; white-space:normal; }}
    .optional-col {{ display:none; }}
    body.show-extra-columns .optional-col {{ display:table-cell; }}
    .status {{ color:var(--brand); margin:12px 0; }}
    .status.error {{ color:var(--danger); }}
    .scroll {{ overflow-x:auto; }}
  </style>
</head>
<body>
  <main>
    <section class='card'>
      <h1>基金估值跟踪</h1>
      <p class='sub'>{page_desc}</p>
      {page_tools}
      <form class='inline' method='post' action='/tracked/add'>
        {phone_hidden_input}
        <input type='text' name='code' placeholder='输入基金代码，例如 161725' list='fund-catalog' required>
        <button type='submit'>加入跟踪</button>
      </form>
      <datalist id='fund-catalog'>{catalog_options}</datalist>
      <form class='inline' method='post' action='/refresh'>
        {phone_hidden_input}
        <button type='submit' {refresh_disabled}>{refresh_label}</button>
      </form>
      <form class='inline' onsubmit='return false;'>
        {extra_columns_toggle}
      </form>
      {status_text}
      <p class='sub'>默认按官方估算涨跌从大到小排序。官方估值会合并东方财富多个分类；若官方估算为空，可点“显示扩展列”查看自算估值，自算会结合历史净值和持仓日线做校准，但仍只适合粗略参考。</p>
    </section>
    <section class='card scroll'>
      <table>
        <thead>
          <tr>
            <th>基金代码</th><th>基金名称</th><th>官方估算值</th><th class='primary-col'>官方估算涨跌</th><th class='optional-col'>自算估值</th><th class='optional-col'>自算涨跌</th><th>昨日增长</th><th>近一月增长</th><th>操作</th>
          </tr>
        </thead>
        <tbody>{table_html}</tbody>
      </table>
    </section>
  </main>
  <script>
    function toggleExtraColumns() {{
      document.body.classList.toggle('show-extra-columns');
      var button = document.getElementById('toggle-extra-columns');
      if (button) {{
        button.textContent = document.body.classList.contains('show-extra-columns') ? '隐藏扩展列' : '显示扩展列';
      }}
    }}
  </script>
</body>
</html>"""


def _normalize_code(code: str) -> str:
    normalized = ''.join(ch for ch in code.strip() if ch.isdigit())
    return normalized.zfill(6) if normalized else ''


def _normalize_phone(phone: str) -> str:
    normalized = ''.join(ch for ch in phone.strip() if ch.isdigit())
    return normalized if len(normalized) == 11 else ''


def _mask_phone(phone: str) -> str:
    if len(phone) != 11:
        return phone
    return f'{phone[:3]}****{phone[-4:]}'


def _user_dir(phone: str) -> Path:
    return USER_DATA_DIR / phone


def _tracked_file(phone: str = '') -> Path:
    return _user_dir(phone) / 'tracked_funds.txt' if phone else TRACKED_FILE


def _cache_file(phone: str = '') -> Path:
    return _user_dir(phone) / 'valuation_cache.json' if phone else CACHE_FILE


def _status_file(phone: str = '') -> Path:
    return _user_dir(phone) / 'refresh_status.json' if phone else STATUS_FILE


def _list_all_user_phones() -> list[str]:
    if not USER_DATA_DIR.exists():
        return []
    phones = []
    for item in USER_DATA_DIR.iterdir():
        if item.is_dir():
            phone = _normalize_phone(item.name)
            if phone:
                phones.append(phone)
    return sorted(set(phones))


def _ensure_user_files(phone: str) -> None:
    normalized_phone = _normalize_phone(phone)
    if not normalized_phone:
        return

    user_dir = _user_dir(normalized_phone)
    user_dir.mkdir(parents=True, exist_ok=True)

    tracked_path = _tracked_file(normalized_phone)
    if not tracked_path.exists():
        tracked_path.write_text('', encoding='utf-8')

    cache_path = _cache_file(normalized_phone)
    if not cache_path.exists():
        cache_path.write_text(json.dumps({'rows': {}, 'refreshed_at': ''}, ensure_ascii=False, indent=2), encoding='utf-8')

    status_path = _status_file(normalized_phone)
    if not status_path.exists():
        _write_status('idle', '等待刷新', phone=normalized_phone)

    _migrate_seeded_user_page(normalized_phone)


def _migrate_seeded_user_page(phone: str) -> None:
    normalized_phone = _normalize_phone(phone)
    if not normalized_phone:
        return

    template_codes = _read_tracked_codes()
    user_codes = _read_tracked_codes(normalized_phone)
    cache_payload = _read_json(_cache_file(normalized_phone), {})
    cached_rows = cache_payload.get('rows', {}) if isinstance(cache_payload, dict) else {}
    refreshed_at = cache_payload.get('refreshed_at', '') if isinstance(cache_payload, dict) else ''

    if not user_codes or user_codes != template_codes or cached_rows or refreshed_at:
        return

    _tracked_file(normalized_phone).write_text('', encoding='utf-8')
    _cache_file(normalized_phone).write_text(json.dumps({'rows': {}, 'refreshed_at': ''}, ensure_ascii=False, indent=2), encoding='utf-8')
    _write_status('idle', '等待添加基金', phone=normalized_phone)


def _read_tracked_codes(phone: str = '') -> list[str]:
    path = _tracked_file(phone)
    if not path.exists():
        return []
    return [line.strip() for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]


def _write_tracked_codes(codes: list[str], phone: str = '') -> None:
    unique_codes: list[str] = []
    for code in codes:
        normalized = _normalize_code(code)
        if normalized and normalized not in unique_codes:
            unique_codes.append(normalized)
    body = '\n'.join(unique_codes)
    if body:
        body += '\n'
    _tracked_file(phone).write_text(body, encoding='utf-8')


def _read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except json.JSONDecodeError:
        return default


def _cache_needs_refresh(path: Path) -> bool:
    payload = _read_json(path, {})
    if not isinstance(payload, dict) or not payload.get('rows'):
        return True
    refreshed_at = str(payload.get('refreshed_at') or '').strip()
    if not refreshed_at:
        return True
    try:
        refreshed_time = datetime.fromisoformat(refreshed_at)
    except ValueError:
        return True
    return (datetime.now() - refreshed_time).total_seconds() >= AUTO_REFRESH_SECONDS


def _reset_stale_status(phone: str = '') -> None:
    status = _read_json(_status_file(phone), {})
    if isinstance(status, dict) and status.get('status') == 'running':
        _write_status('error', '服务重启后已清理未完成的刷新任务', phone=phone)


def _write_status(status: str, message: str, updated_at: str = '', phone: str = '') -> None:
    payload = {'status': status, 'message': message, 'updated_at': updated_at or _now()}
    _status_file(phone).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def _read_catalog_name(code: str) -> str:
    payload = _read_json(CATALOG_FILE, {})
    for item in payload.get('items', []):
        if str(item.get('code', '')).zfill(6) == code:
            return str(item.get('name', '')).strip()
    return ''


def _maybe_refresh_catalog(rows: dict[str, dict]) -> None:
    payload = _read_json(CATALOG_FILE, {})
    items = payload.get('items', []) if isinstance(payload, dict) else []
    existing = {str(item.get('code', '')).zfill(6): str(item.get('name', '')).strip() for item in items}
    changed = False
    for code, row in rows.items():
        name = str(row.get('name', '')).strip()
        if code and name and existing.get(code) != name:
            existing[code] = name
            changed = True
    if changed or not items:
        merged = [{'code': code, 'name': name} for code, name in sorted(existing.items()) if code and name]
        CATALOG_FILE.write_text(json.dumps({'items': merged}, ensure_ascii=False, indent=2), encoding='utf-8')


def _pick_column(columns: list[str], keyword: str) -> str:
    for column in columns:
        if keyword in str(column):
            return str(column)
    return ''


def _sort_nav_frame(frame):
    date_col = _pick_column(frame.columns.tolist(), '净值日期') or _pick_column(frame.columns.tolist(), '日期')
    if not date_col:
        return frame
    try:
        return frame.sort_values(date_col)
    except Exception:
        return frame


def _calc_recent_growth(records: list[dict], nav_col: str, date_col: str = '') -> float | None:
    points: list[tuple[datetime | None, float]] = []
    for record in records:
        nav = _as_float(record.get(nav_col))
        if nav is None:
            continue
        date_value = _parse_date(record.get(date_col)) if date_col else None
        points.append((date_value, nav))
    if len(points) < 2:
        return None

    latest_date, latest_nav = points[-1]
    start_nav = None
    if latest_date is not None:
        cutoff = latest_date - timedelta(days=30)
        for point_date, nav in points:
            if point_date is not None and point_date <= cutoff:
                start_nav = nav
            elif point_date is not None and point_date > cutoff:
                break
    if start_nav is None:
        start_nav = points[-22][1] if len(points) >= 22 else points[0][1]
    return _calc_pct(latest_nav, start_nav)


def _parse_date(value) -> datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10] if '-' in text else text[:8], fmt)
        except ValueError:
            continue
    return None


def _as_float(value):
    if value is None:
        return None
    text = str(value).strip().replace(',', '').replace('%', '')
    if not text or text == '---' or text.lower() == 'nan':
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _calc_pct(current, previous):
    current_value = _as_float(current)
    previous_value = _as_float(previous)
    if current_value is None or previous_value in {None, 0}:
        return None
    return (current_value / previous_value - 1.0) * 100.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _history_window_return(history: list[dict], days: int) -> float | None:
    values = []
    for item in history or []:
        number = _as_float(item.get('nav'))
        if number is not None:
            values.append(number)
    if len(values) <= days:
        return None
    return _calc_pct(values[-1], values[-(days + 1)])


def _format_value(value) -> str:
    number = _as_float(value)
    return f'{number:.4f}' if number is not None else '---'


def _format_percent(value) -> str:
    number = _as_float(value)
    return f'{number:.2f}%' if number is not None else '---'


def _blank_if_empty(value) -> str:
    text = str(value or '').strip()
    return '' if text in {'', '---', 'None', 'nan'} else text


def _tone_class(value) -> str:
    number = _as_float(value)
    if number is None:
        return ''
    if number > 0:
        return 'up'
    if number < 0:
        return 'down'
    return ''


def _sort_codes_by_estimate_growth(tracked_codes: list[str], cached_rows: dict[str, dict]) -> list[str]:
    def sort_key(code: str) -> tuple[bool, float]:
        value = _as_float(cached_rows.get(code, {}).get('estimate_growth'))
        return (value is not None, value if value is not None else float('-inf'))

    return sorted(tracked_codes, key=sort_key, reverse=True)


def _now() -> str:
    return datetime.now().isoformat(timespec='seconds')


def _log(message: str) -> None:
    print(f'[{_now()}] {message}', flush=True)


def _start_auto_refresh_loop() -> None:
    def worker() -> None:
        while True:
            time.sleep(AUTO_REFRESH_SECONDS)
            _trigger_refresh()
            for phone in _list_all_user_phones():
                _trigger_refresh(phone)
    threading.Thread(target=worker, daemon=True).start()


if __name__ == '__main__':
    main()

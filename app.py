from __future__ import annotations

import json
import multiprocessing as mp
import os
import threading
import time
from datetime import datetime
from functools import partial
from html import escape
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs

import akshare as ak

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
HOLDINGS_TIMEOUT_SECONDS = int(os.getenv('FUND_HOLDINGS_TIMEOUT_SECONDS', '12'))
STOCK_SPOT_TIMEOUT_SECONDS = int(os.getenv('FUND_STOCK_SPOT_TIMEOUT_SECONDS', '18'))
QUOTE_CACHE_TTL_SECONDS = int(os.getenv('FUND_QUOTE_CACHE_TTL_SECONDS', '300'))
ESTIMATION_CACHE_TTL_SECONDS = int(os.getenv('FUND_ESTIMATION_CACHE_TTL_SECONDS', '300'))

_refresh_lock = threading.Lock()
_tushare_lock = threading.Lock()
_tushare_pro = None
_quote_cache = {'updated_at': 0.0, 'rows': {}}
_estimation_cache = {'updated_at': 0.0, 'rows': {}}


class FundHandler(SimpleHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in {'/', ''}:
            self.send_error(404, 'Not Found')
            return

        tracked_codes = _read_tracked_codes()
        cache_payload = _read_json(CACHE_FILE, {})
        cached_rows = cache_payload.get('rows', {}) if isinstance(cache_payload, dict) else {}
        refreshed_at = cache_payload.get('refreshed_at', '') if isinstance(cache_payload, dict) else ''
        status = _read_json(STATUS_FILE, {})
        catalog_payload = _read_json(CATALOG_FILE, {})
        catalog = catalog_payload.get('items', []) if isinstance(catalog_payload, dict) else []
        html = _render_page(tracked_codes, cached_rows, refreshed_at, status, catalog)

        encoded = html.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_POST(self) -> None:
        content_length = int(self.headers.get('Content-Length', '0'))
        raw = self.rfile.read(content_length).decode('utf-8')
        form = parse_qs(raw)
        code = _normalize_code(form.get('code', [''])[0])

        if self.path == '/refresh':
            _trigger_refresh()
            return self._redirect_home()

        if self.path == '/tracked/add' and code:
            tracked = _read_tracked_codes()
            if code not in tracked:
                tracked.append(code)
                _write_tracked_codes(tracked)
            _trigger_refresh()
            return self._redirect_home()

        if self.path == '/tracked/delete' and code:
            tracked = [item for item in _read_tracked_codes() if item != code]
            _write_tracked_codes(tracked)
            _trigger_refresh()
            return self._redirect_home()

        self.send_error(400, 'Unsupported request')

    def _redirect_home(self) -> None:
        self.send_response(303)
        self.send_header('Location', '/')
        self.end_headers()


def main() -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    TRACKED_FILE.touch(exist_ok=True)
    if not CACHE_FILE.exists():
        CACHE_FILE.write_text(json.dumps({'rows': {}, 'refreshed_at': ''}, ensure_ascii=False, indent=2), encoding='utf-8')
    if not STATUS_FILE.exists():
        _write_status('idle', '等待刷新')
    if not CATALOG_FILE.exists():
        CATALOG_FILE.write_text(json.dumps({'items': []}, ensure_ascii=False, indent=2), encoding='utf-8')

    if not _read_json(CACHE_FILE, {}).get('rows'):
        _trigger_refresh()
    _start_auto_refresh_loop()

    handler = partial(FundHandler, directory=str(BASE_DIR))
    server = ThreadingHTTPServer((HOST, PORT), handler)
    server.serve_forever()


def _trigger_refresh() -> None:
    if _refresh_lock.locked():
        _write_status('running', '已有基金估值刷新任务在执行')
        return

    def worker() -> None:
        with _refresh_lock:
            try:
                _write_status('running', '正在刷新基金估值')
                rows = _fetch_tracked_rows(_read_tracked_codes())
                CACHE_FILE.write_text(
                    json.dumps({'rows': rows, 'refreshed_at': _now()}, ensure_ascii=False, indent=2),
                    encoding='utf-8',
                )
                _write_status('success', f'已刷新 {len(rows)} 只基金', _now())
            except Exception as exc:
                _write_status('error', str(exc))

    threading.Thread(target=worker, daemon=True).start()


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
        holdings, holdings_source = _fetch_holdings(code)
        quotes, quote_source = _fetch_stock_quotes([item['stock_code'] for item in holdings[:10]])
        self_estimate = _estimate_by_holdings(nav_item, holdings, quotes)
        name = nav_item.get('name') or estimate_item.get('name') or catalog_name_map.get(code) or code
        source_parts = []
        if estimate_item.get('estimate_value') is not None:
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
            'deviation': _format_percent(estimate_item.get('deviation')),
            'self_estimate_value': _format_value(self_estimate.get('value') if self_estimate else None),
            'self_estimate_growth': _format_percent(self_estimate.get('growth') if self_estimate else None),
            'holdings_coverage': _format_percent(self_estimate.get('coverage') if self_estimate else None),
            'holding_count': int(self_estimate.get('holding_count') or 0) if self_estimate else 0,
            'estimate_source': ' | '.join(source_parts),
        }

    _maybe_refresh_catalog(rows)
    return rows


def _fetch_nav_info(code: str) -> dict[str, float | str | None]:
    name = _read_catalog_name(code)
    try:
        frame = ak.fund_open_fund_info_em(symbol=code, indicator='单位净值走势', period='1月')
        if not frame.empty:
            tail = frame.tail(1).to_dict(orient='records')[0]
            return {
                'name': name,
                'published_nav': _as_float(tail.get('单位净值')),
                'published_growth': _as_float(tail.get('日增长率')),
            }
    except Exception:
        pass

    pro = _get_tushare_pro()
    if not pro:
        return {'name': name, 'published_nav': None, 'published_growth': None}
    for ts_code in _candidate_ts_codes(code):
        try:
            df = pro.fund_nav(ts_code=ts_code, limit=1)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        row = df.iloc[0].to_dict()
        return {
            'name': name,
            'published_nav': _as_float(row.get('unit_nav')),
            'published_growth': _calc_pct(row.get('unit_nav'), row.get('pre_unit_nav')),
        }
    return {'name': name, 'published_nav': None, 'published_growth': None}


def _fetch_estimation_map_cached() -> dict[str, dict]:
    now = time.time()
    if now - float(_estimation_cache.get('updated_at', 0.0)) < ESTIMATION_CACHE_TTL_SECONDS:
        return dict(_estimation_cache.get('rows', {}))

    estimate_map: dict[str, dict] = {}
    try:
        frame = ak.fund_value_estimation_em()
    except Exception:
        frame = None
    if frame is not None and not frame.empty:
        columns = frame.columns.tolist()
        code_col = _pick_column(columns, '基金代码')
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
            estimate_map[code] = {
                'name': str(record.get(name_col, '') if name_col else '').strip(),
                'estimate_value': _as_float(record.get(estimate_value_col)) if estimate_value_col else None,
                'estimate_growth': _as_float(record.get(estimate_growth_col)) if estimate_growth_col else None,
                'deviation': _as_float(record.get(deviation_col)) if deviation_col else None,
            }
    _estimation_cache['updated_at'] = now
    _estimation_cache['rows'] = estimate_map
    return dict(estimate_map)


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
                frame = ak.fund_portfolio_hold_em(symbol=code, date=year)
            except Exception:
                continue
            if frame is None or frame.empty:
                continue
            code_col = _pick_column(frame.columns.tolist(), '股票代码') or _pick_column(frame.columns.tolist(), '代码')
            weight_col = _pick_column(frame.columns.tolist(), '占净值比例') or _pick_column(frame.columns.tolist(), '占净值比')
            name_col = _pick_column(frame.columns.tolist(), '股票名称') or _pick_column(frame.columns.tolist(), '名称')
            if not code_col or not weight_col:
                continue
            items = []
            for record in frame.to_dict(orient='records'):
                stock_code = _normalize_code(str(record.get(code_col, '')))
                weight = _as_float(record.get(weight_col))
                if not stock_code or weight is None or weight <= 0:
                    continue
                items.append({'stock_code': stock_code, 'stock_name': str(record.get(name_col, '') if name_col else '').strip(), 'weight': weight})
            items.sort(key=lambda item: float(item['weight']), reverse=True)
            queue.put(items)
            return
        queue.put([])
    except Exception:
        queue.put([])


def _fetch_holdings_tushare(code: str) -> list[dict]:
    pro = _get_tushare_pro()
    if not pro:
        return []
    best = []
    for ts_code in _candidate_ts_codes(code):
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

    quotes = _fetch_stock_quotes_akshare(needed)
    if quotes:
        _quote_cache['updated_at'] = now
        _quote_cache['rows'] = {**cached_rows, **quotes}
        return quotes, 'akshare'

    quotes = _fetch_stock_quotes_tushare(needed)
    if quotes:
        _quote_cache['updated_at'] = now
        _quote_cache['rows'] = {**cached_rows, **quotes}
        return quotes, 'tushare'

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
    ts_codes = ','.join(_candidate_ts_codes(code)[0] for code in codes)
    try:
        df = ts.realtime_quote(ts_code=ts_codes)
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    rows = {}
    for record in df.to_dict(orient='records'):
        raw_ts_code = str(record.get('TS_CODE') or record.get('ts_code') or '').strip()
        code = _normalize_code(raw_ts_code.split('.')[0])
        if not code:
            continue
        price = _as_float(record.get('PRICE') or record.get('price') or record.get('current'))
        pre_close = _as_float(record.get('PRE_CLOSE') or record.get('pre_close'))
        pct_change = _as_float(record.get('PCT_CHANGE') or record.get('pct_change'))
        if pct_change is None:
            pct_change = _calc_pct(price, pre_close)
        rows[code] = {'name': str(record.get('NAME') or record.get('name') or '').strip(), 'pct_change': pct_change}
    return rows


def _estimate_by_holdings(nav_item: dict, holdings: list[dict], quotes: dict[str, dict]) -> dict | None:
    published_nav = _as_float(nav_item.get('published_nav'))
    if published_nav is None or not holdings or not quotes:
        return None
    weighted_return = 0.0
    covered_weight = 0.0
    holding_count = 0
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
        weight = _as_float(item.get('weight'))
        if pct_change is None or weight is None or weight <= 0:
            continue
        weighted_return += (weight / 100.0) * (pct_change / 100.0)
        covered_weight += weight
        holding_count += 1
    if holding_count == 0 or covered_weight <= 0:
        return None
    return {
        'value': published_nav * (1.0 + weighted_return),
        'growth': weighted_return * 100.0,
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


def _render_page(tracked_codes: list[str], cached_rows: dict[str, dict], refreshed_at: str, status: dict, catalog: list[dict]) -> str:
    if status.get('status') == 'running':
        status_text = f"<p class='status'>基金估值刷新中，最近触发时间 {escape(str(status.get('updated_at', '')))}。</p>"
    elif status.get('status') == 'success':
        status_text = f"<p class='status'>最近一次刷新成功：{escape(str(status.get('message', '')))}。 完成时间 {escape(str(status.get('updated_at', '')))}。</p>"
    elif status.get('status') == 'error':
        status_text = f"<p class='status error'>刷新失败：{escape(str(status.get('message', '未知错误')))}</p>"
    else:
        status_text = ''

    refresh_disabled = 'disabled' if status.get('status') == 'running' else ''
    refresh_label = '刷新中...' if status.get('status') == 'running' else '立即刷新估值'
    catalog_options = '\n'.join(
        f"<option value=\"{escape(str(item.get('code', '')))}\">{escape(str(item.get('code', '')))} {escape(str(item.get('name', '')))}</option>"
        for item in catalog[:800]
    )
    rows_html = []
    for code in tracked_codes:
        row = cached_rows.get(code, {})
        rows_html.append(
            f"""
            <tr>
              <td>{escape(code)}</td>
              <td>{escape(str(row.get('name', '---')))}</td>
              <td>{escape(str(row.get('estimate_value', '---')))}</td>
              <td>{escape(str(row.get('estimate_growth', '---')))}</td>
              <td>{escape(str(row.get('self_estimate_value', '---')))}</td>
              <td>{escape(str(row.get('self_estimate_growth', '---')))}</td>
              <td>{escape(str(row.get('holdings_coverage', '---')))}</td>
              <td>{escape(str(row.get('published_nav', '---')))}</td>
              <td>{escape(str(row.get('published_growth', '---')))}</td>
              <td>{escape(str(row.get('deviation', '---')))}</td>
              <td>{escape(str(row.get('estimate_source', '---')))}</td>
              <td>
                <form method='post' action='/tracked/delete'>
                  <input type='hidden' name='code' value='{escape(code)}'>
                  <button type='submit' class='danger'>删除</button>
                </form>
              </td>
            </tr>
            """
        )
    table_html = '\n'.join(rows_html) or "<tr><td colspan='12'>当前没有跟踪的基金。</td></tr>"
    meta_refresh = "<meta http-equiv='refresh' content='8'>" if status.get('status') == 'running' else ''
    return f"""<!doctype html>
<html lang='zh-CN'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  {meta_refresh}
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
    table {{ width:100%; border-collapse:collapse; }}
    th,td {{ border-bottom:1px solid #eaecf0; padding:10px 8px; text-align:left; font-size:14px; white-space:nowrap; }}
    th {{ color:var(--muted); }}
    .status {{ color:var(--brand); margin:12px 0; }}
    .status.error {{ color:var(--danger); }}
    .scroll {{ overflow-x:auto; }}
  </style>
</head>
<body>
  <main>
    <section class='card'>
      <h1>基金估值跟踪</h1>
      <p class='sub'>端口 11452 | 最近缓存时间: {escape(refreshed_at or '暂无')} | 自动刷新间隔: {AUTO_REFRESH_SECONDS // 60} 分钟</p>
      <form class='inline' method='post' action='/tracked/add'>
        <input type='text' name='code' placeholder='输入基金代码，例如 161725' list='fund-catalog' required>
        <button type='submit'>加入跟踪</button>
      </form>
      <datalist id='fund-catalog'>{catalog_options}</datalist>
      <form class='inline' method='post' action='/refresh'><button type='submit' {refresh_disabled}>{refresh_label}</button></form>
      {status_text}
      <p class='sub'>AkShare 和 Tushare 会按顺序回退尝试；成功一个就用，并带缓存节流，避免请求过于频繁。</p>
    </section>
    <section class='card scroll'>
      <table>
        <thead>
          <tr>
            <th>基金代码</th><th>基金名称</th><th>官方估算值</th><th>官方估算涨跌</th><th>自算估值</th><th>自算涨跌</th><th>持仓覆盖</th><th>公布净值</th><th>公布日增长率</th><th>估算偏差</th><th>估值来源</th><th>操作</th>
          </tr>
        </thead>
        <tbody>{table_html}</tbody>
      </table>
    </section>
  </main>
</body>
</html>"""


def _normalize_code(code: str) -> str:
    normalized = ''.join(ch for ch in code.strip() if ch.isdigit())
    return normalized.zfill(6) if normalized else ''


def _read_tracked_codes() -> list[str]:
    if not TRACKED_FILE.exists():
        return []
    return [line.strip() for line in TRACKED_FILE.read_text(encoding='utf-8').splitlines() if line.strip()]


def _write_tracked_codes(codes: list[str]) -> None:
    unique_codes: list[str] = []
    for code in codes:
        normalized = _normalize_code(code)
        if normalized and normalized not in unique_codes:
            unique_codes.append(normalized)
    body = '\n'.join(unique_codes)
    if body:
        body += '\n'
    TRACKED_FILE.write_text(body, encoding='utf-8')


def _read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except json.JSONDecodeError:
        return default


def _write_status(status: str, message: str, updated_at: str = '') -> None:
    payload = {'status': status, 'message': message, 'updated_at': updated_at or _now()}
    STATUS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


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


def _format_value(value) -> str:
    number = _as_float(value)
    return f'{number:.4f}' if number is not None else '---'


def _format_percent(value) -> str:
    number = _as_float(value)
    return f'{number:.2f}%' if number is not None else '---'


def _now() -> str:
    return datetime.now().isoformat(timespec='seconds')


def _start_auto_refresh_loop() -> None:
    def worker() -> None:
        while True:
            time.sleep(AUTO_REFRESH_SECONDS)
            _trigger_refresh()
    threading.Thread(target=worker, daemon=True).start()


if __name__ == '__main__':
    main()

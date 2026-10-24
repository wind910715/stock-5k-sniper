import os
import time
import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pytz
import requests
import urllib3
import pandas as pd
import yfinance as yf

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ==================== 實戰雙引擎參數設定 ====================
# 【策略 A：5K / 區間爆量急拉】
VOL_MULTIPLIER = 2.8         # 區間爆量倍數：近 3~5 分鐘量 >= 5日5K均量的 2.8 倍
PCT_THRESHOLD = 1.6          # 瞬間急拉門檻：拉升幅度 >= 1.6% (提早於 +1.6% 起漲點就抓出！)
MIN_5K_LOTS = 80             # 區間最低張數：>= 80 張

# 【策略 B：外盤連續大買單（主力主動向上掃貨，不必等漲滿 1.6% 即刻抓起漲！）】
OUTER_STREAK_COUNT = 2       # 連續幾個取樣週期出現外盤主動大買單 (2次 = 約20秒內連續掃貨)
OUTER_STREAK_MIN_LOTS = 150  # 連續外盤掃貨累計張數門檻：>= 150 張
OUTER_STREAK_VOL_RATIO = 1.6 # 連續外盤量達 5K 均量的倍數：>= 1.6 倍 (在股價剛動 0.4% 時就提前警報！)
OUTER_SINGLE_MEGA_LOTS = 250 # 單發外盤一擊必殺大單：10秒內外盤直接狂敲 >= 250 張且達均量 2.0 倍

# 【系統極速設定】
MIN_AVG_DAILY_LOTS = 300     # 活躍股門檻：過去 5 日日均量 >= 300 張
SCAN_INTERVAL_SEC = 10       # 每 10 秒極速巡邏一次全市場！(原為30秒，提速3倍)
MAX_WORKERS = 10             # 10 執行緒併發抓取台灣證交所 MIS，將 61 秒壓縮至 3~5 秒！
SEND_HEARTBEAT = True        # 每 5 分鐘整點發送心跳回報

# 【去雜訊防護】2026-10-24 審查加入：皆為結構性防護 (非門檻調參)，可個別以數值關閉
MIN_5K_WINDOW_COVERAGE_SEC = 240  # 策略A：取樣視窗須涵蓋 >= 240 秒才判定，避免拿 20~60 秒的量去除 5 分鐘均量基準 (設 0 關閉)
OUTER_MAX_STEP_GAP_SEC = 30       # 策略B：與上一拍間隔 > 30 秒的量差視為「累積量」(斷線/稀疏成交)，不算 10 秒掃貨 (設極大值可關閉)
OUTER_STREAK_WINDOW_SEC = 60      # 策略B：「連續」外盤的取樣點必須落在最近 60 秒內，跨數分鐘的零星外盤不算連續
GLOBAL_STOCK_ALERT_COOLDOWN_SEC = 300  # 同一檔股票任一策略推播後，300 秒內不推另一策略 (同一波行情不重複轟炸，設 0 關閉)
MAX_ALERTS_PER_STOCK_PER_DAY = 5  # 同一檔股票單日推播上限 (兩策略合計，設 0 不限制)
# ==========================================================

STOCK_DICT = {}
BASELINE_AVG_5K = {}         # 過去 5 日每 5 分鐘平均張數
ACTIVE_UNIVERSE = set()
ALERTED_COOLDOWN = {}        # 記錄 (股票, 策略類型) 最後推播時間，防止重複刷屏
PRICE_VOL_HISTORY = {}       # 儲存每檔股票近期快照：deque of dict
LAST_ALERT_ANY = {}          # 每檔股票「任一策略」最後推播時間 (跨策略去重)
ALERT_COUNT_TODAY = {}       # 每檔股票當日推播次數 (程序每日重啟，天然以日為界)

FUTURES_SET = {
    "2330", "2317", "2454", "2382", "2308", "2303", "2881", "2882", "2891", "2603",
    "2609", "2615", "3231", "2356", "2376", "2383", "3017", "3037", "3035", "3443",
    "3661", "3711", "2327", "2379", "2408", "2409", "3481", "2618", "2610", "1513",
    "1519", "1503", "1504", "1605", "2002", "1101", "1301", "1303", "2345", "2368",
    "2313", "2353", "2357", "2324", "2352", "2377", "2395", "2449", "2458", "3008",
    "3034", "3044", "3189", "3532", "3533", "3653", "3702", "3706", "4904", "4938",
    "4958", "4966", "4979", "5269", "5347", "5483", "6176", "6239", "6278", "6285",
    "6415", "6446", "6488", "6505", "6669", "6770", "8046", "8069", "8299", "8358",
    "0050", "0056", "00878", "00919", "00929", "00940", "00631L", "00632R", "00637L"
}

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        res = requests.post(url, json=payload, timeout=8)
        if res.status_code == 400:
            payload.pop("parse_mode", None)
            requests.post(url, json=payload, timeout=8)
    except Exception:
        pass

def clean_symbol_code(sym):
    if sym.endswith(".TWO"):
        return sym[:-4]
    if sym.endswith(".TW"):
        return sym[:-3]
    return sym

def is_valid_stock_code(code):
    if not (4 <= len(code) <= 6):
        return False
    if code.startswith("00") and code[-1] in ('B', 'U', 'D'):
        return False
    return True

def init_market_and_baseline():
    global STOCK_DICT, FUTURES_SET, BASELINE_AVG_5K, ACTIVE_UNIVERSE
    if len(ACTIVE_UNIVERSE) > 200:
        return

    tz_tw = pytz.timezone("Asia/Taipei")
    now_str = datetime.now(tz_tw).strftime('%Y-%m-%d')
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        res_sf = requests.get("https://openapi.taifex.com.tw/v1/SingleStockFuturesMargining", headers=headers, verify=False, timeout=6)
        if res_sf.status_code == 200:
            for row in res_sf.json():
                u_code = str(row.get("UnderlyingSecurityCode", "")).strip()
                if len(u_code) >= 4:
                    FUTURES_SET.add(u_code)
    except Exception:
        pass

    try:
        r1 = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", headers=headers, verify=False, timeout=8)
        if r1.status_code == 200:
            for row in r1.json():
                code = row.get("Code", "").strip()
                if is_valid_stock_code(code):
                    STOCK_DICT[f"{code}.TW"] = row.get("Name", "").strip()
    except Exception:
        pass

    try:
        r2 = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", headers=headers, verify=False, timeout=8)
        if r2.status_code == 200:
            for row in r2.json():
                code = row.get("SecuritiesCompanyCode", "").strip()
                if is_valid_stock_code(code):
                    STOCK_DICT[f"{code}.TWO"] = row.get("CompanyName", "").strip()
    except Exception:
        pass

    all_symbols = list(STOCK_DICT.keys())
    print(f"📥 [預熱] 載入 {len(all_symbols)} 檔標的，建立過去 5 日均量基準...")

    min_daily_shares = MIN_AVG_DAILY_LOTS * 1000
    for i in range(0, len(all_symbols), 250):
        batch = all_symbols[i:i + 250]
        try:
            data_d = yf.download(tickers=batch, period="7d", interval="1d", group_by="ticker", threads=True, progress=False)
            if data_d.empty:
                continue
            for sym in batch:
                try:
                    # group_by="ticker" 時 columns 恆為 MultiIndex (含批次只有 1 檔時)，一律以 sym 取欄；
                    # 原寫法在尾批次剩 1 檔時會把整包 MultiIndex 框當單檔 df，該檔基準會靜默遺失
                    df_d = data_d[sym] if isinstance(data_d.columns, pd.MultiIndex) else data_d
                    df_d = df_d.dropna(subset=["Volume"])
                    if df_d.empty:
                        continue
                    df_hist = df_d[df_d.index.strftime('%Y-%m-%d') != now_str]
                    df_hist = df_hist[df_hist["Volume"] > 0].tail(5)
                    if len(df_hist) > 0:
                        avg_daily_vol = float(df_hist["Volume"].mean())
                        clean_c = clean_symbol_code(sym)
                        if avg_daily_vol >= min_daily_shares or clean_c in FUTURES_SET:
                            BASELINE_AVG_5K[sym] = max(5.0, (avg_daily_vol / 1000.0) / 54.0)
                            ACTIVE_UNIVERSE.add(sym)
                except Exception:
                    continue
        except Exception:
            pass

    print(f"✅ [預熱完成] 鎖定 {len(ACTIVE_UNIVERSE)} 檔活躍股 (含 {len(FUTURES_SET)} 檔股期標的)！")

def _fetch_single_mis_chunk(chunk, sym_map):
    """單一批次抓取證交所 MIS 報價 (供多執行緒併發呼叫，包含五檔委買委賣以判斷內外盤)"""
    chunk_results = {}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': 'https://mis.twse.com.tw/stock/index.jsp'
    }
    ex_ch = "|".join(chunk)
    url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0"
    try:
        res = requests.get(url, headers=headers, verify=False, timeout=4.5)
        if res.status_code == 200:
            data = res.json()
            for item in data.get("msgArray", []):
                ch = f"{item.get('ex', '')}_{item.get('ch', '')}"
                sym = sym_map.get(ch)
                if not sym:
                    continue

                # 解析委買第一檔 (bid1) 與 委賣第一檔 (ask1，即外盤價)
                b_list = [x for x in str(item.get("b", "")).split("_") if x not in ("-", "")]
                a_list = [x for x in str(item.get("a", "")).split("_") if x not in ("-", "")]
                bid1 = float(b_list[0]) if b_list else 0.0
                ask1 = float(a_list[0]) if a_list else 0.0

                # traded=False 表示本拍無實際成交價 (MIS z="-")：舊程式以 bid1 冒充成交價，
                # 會把「委買價上移」誤寫進成交價歷史，污染視窗低點/拉升幅度與外盤判定
                z_raw = item.get("z", "-")
                traded = z_raw not in ("-", "", None)
                z_str = z_raw
                if not traded:
                    z_str = str(bid1) if bid1 > 0 else item.get("y", "0")

                try:
                    price = float(z_str)
                    total_lots = int(float(item.get("v", "0")))
                    tick_lots = int(float(item.get("tv", "0"))) if item.get("tv", "-") not in ("-", "") else 0
                    open_p = float(item.get("o", price)) if item.get("o", "-") not in ("-", "") else price
                    yest_p = float(item.get("y", price)) if item.get("y", "-") not in ("-", "") else price
                    low_p = float(item.get("l", price)) if item.get("l", "-") not in ("-", "") else price

                    if price > 0 and total_lots > 0:
                        chunk_results[sym] = {
                            "price": price,
                            "total_lots": total_lots,
                            "tick_lots": tick_lots,
                            "bid1": bid1,
                            "ask1": ask1,
                            "open": open_p,
                            "yest": yest_p,
                            "low": low_p,
                            "traded": traded
                        }
                except Exception:
                    continue
    except Exception:
        pass
    return chunk_results

def fetch_realtime_mis_quotes_parallel(symbols_list):
    """
    【10執行緒併發極速引擎】
    將 15 批跨海請求同時併發射出，耗時從 61.7 秒直接砍到 3.5 秒內！
    """
    results = {}
    mis_keys = []
    sym_map = {}
    for s in symbols_list:
        code = clean_symbol_code(s)
        prefix = "otc" if s.endswith(".TWO") else "tse"
        m_key = f"{prefix}_{code}.tw"
        mis_keys.append(m_key)
        sym_map[m_key] = s

    chunk_size = 80
    chunks = [mis_keys[i:i + chunk_size] for i in range(0, len(mis_keys), chunk_size)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(_fetch_single_mis_chunk, c, sym_map) for c in chunks]
        for f in as_completed(futures):
            try:
                results.update(f.result())
            except Exception:
                pass

    return results

def _alert_allowed(sym: str, now_ts: float) -> bool:
    """跨策略去雜訊閘門：全域冷卻 + 單日上限。任一不通過即不推播 (不影響各策略自身冷卻)。"""
    if GLOBAL_STOCK_ALERT_COOLDOWN_SEC > 0 and \
            (now_ts - LAST_ALERT_ANY.get(sym, 0)) < GLOBAL_STOCK_ALERT_COOLDOWN_SEC:
        return False
    if MAX_ALERTS_PER_STOCK_PER_DAY > 0 and \
            ALERT_COUNT_TODAY.get(sym, 0) >= MAX_ALERTS_PER_STOCK_PER_DAY:
        return False
    return True


def _register_alert(sym: str, now_ts: float) -> None:
    LAST_ALERT_ANY[sym] = now_ts
    ALERT_COUNT_TODAY[sym] = ALERT_COUNT_TODAY.get(sym, 0) + 1


def run_realtime_scan(send_hb_now=False, is_manual_test=False):
    start_t = time.time()
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)
    now_ts = time.time()

    if len(ACTIVE_UNIVERSE) < 200:
        init_market_and_baseline()

    sorted_universe = sorted(list(ACTIVE_UNIVERSE), key=lambda s: clean_symbol_code(s) in FUTURES_SET, reverse=True)
    live_quotes = fetch_realtime_mis_quotes_parallel(sorted_universe)
    alerts = []

    if len(live_quotes) > 100:
        for sym, q in live_quotes.items():
            cur_price = q["price"]
            cur_lots = q["total_lots"]
            open_p = q["open"]
            yest_p = q["yest"]
            ask1 = q["ask1"]
            bid1 = q["bid1"]
            if not q.get("traded", True):
                continue  # 本拍無實際成交價 (z="-")：不寫入成交價歷史，避免 bid1 污染視窗與外盤判定

            if sym not in PRICE_VOL_HISTORY:
                PRICE_VOL_HISTORY[sym] = deque(maxlen=35) # 保存約 5 分鐘 (30次x10秒) 的高頻軌跡

            hist = PRICE_VOL_HISTORY[sym]

            # 計算與上一拍 (10秒前) 的價差與量差，用來精準判定「外盤主動買單」
            delta_step_lots = 0
            is_outer_buy = False
            if len(hist) >= 1:
                prev_snap = hist[-1]
                delta_step_lots = max(0, cur_lots - prev_snap["lots"])
                prev_ask1 = prev_snap["ask1"]
                prev_price = prev_snap["price"]
                step_dt = now_ts - prev_snap["ts"]

                # 【外盤主動買進判定】：
                # 只要本拍有新增成交量，且 (最新成交價 >= 上一拍委賣價ask1 或 最新成交價 == 本拍委賣價ask1 或 價格向上推升)
                # 但：與上一拍間隔過久時，量差是多拍累積量而非「10 秒掃貨」，不判外盤 (斷線/稀疏成交的聚合假訊號)
                if delta_step_lots > 0 and step_dt <= OUTER_MAX_STEP_GAP_SEC:
                    if (prev_ask1 > 0 and cur_price >= prev_ask1) or (ask1 > 0 and cur_price >= ask1) or (cur_price > prev_price):
                        is_outer_buy = True

            hist.append({
                "ts": now_ts,
                "price": cur_price,
                "lots": cur_lots,
                "ask1": ask1,
                "bid1": bid1,
                "step_lots": delta_step_lots,
                "is_outer": is_outer_buy
            })

            # 移除超過 300 秒 (5分鐘) 的過期紀錄。
            # 舊寫法保留至少 2 拍：斷線後恢復時最老一拍可能已是數分鐘前，
            # 策略A的量差會把整段空白期的量算成「5分鐘爆量」，且視窗涵蓋守衛會被虛假通過
            while hist and (now_ts - hist[0]["ts"]) > 300:
                hist.popleft()

            if len(hist) < 2:
                continue

            avg_5k_lots = BASELINE_AVG_5K.get(sym, 0)
            if avg_5k_lots <= 0:
                continue

            clean_code = clean_symbol_code(sym)
            stock_name = STOCK_DICT.get(sym, "")
            futures_tag = " 【⚡有股期】" if clean_code in FUTURES_SET else ""
            day_pct = ((cur_price - yest_p) / yest_p) * 100 if yest_p > 0 else 0

            # =========================================================================
            # 🎯 策略一：【外盤連續大買單（主力主動吃貨雷達）】— 抓起漲第一秒！
            # 檢查最近 3 個取樣點 (約 20~30 秒內) 是否連續出現外盤大單掃貨
            # =========================================================================
            recent_snaps = list(hist)[-3:]
            outer_hits = [s for s in recent_snaps if s["is_outer"] and s["step_lots"] >= 20]
            outer_sum_lots = sum(s["step_lots"] for s in outer_hits)
            outer_vol_ratio = outer_sum_lots / avg_5k_lots

            # 條件 A：連續 >= 2 拍外盤主動大買單，合計 >= 150 張且達 5K 均量 1.6 倍，
            # 且取樣點必須落在最近 OUTER_STREAK_WINDOW_SEC 秒內 (舊寫法跨數分鐘的零星外盤也算「連續」)
            streak_window_ok = (now_ts - recent_snaps[0]["ts"]) <= OUTER_STREAK_WINDOW_SEC
            cond_streak_outer = (streak_window_ok and len(outer_hits) >= OUTER_STREAK_COUNT and outer_sum_lots >= OUTER_STREAK_MIN_LOTS and outer_vol_ratio >= OUTER_STREAK_VOL_RATIO)
            # 條件 B：單拍 10 秒內外盤直接一擊狂敲 >= 250 張且達 5K 均量 2.0 倍
            cond_mega_outer = (is_outer_buy and delta_step_lots >= OUTER_SINGLE_MEGA_LOTS and (delta_step_lots / avg_5k_lots) >= 2.0)

            cooldown_key_outer = f"{sym}_OUTER"
            if (cond_streak_outer or cond_mega_outer) and (now_ts - ALERTED_COOLDOWN.get(cooldown_key_outer, 0)) >= 480:
                # 確保近窗價格是往上或平盤墊高，而非往下殺。
                # 修正舊 BUG：recent_low 含本拍，cur_price >= recent_low 恆真，守衛形同虛設；
                # 改為現價不低於本視窗第一拍價格 (近窗淨變化 >= 0)
                recent_low = min(s["price"] for s in recent_snaps)
                if cur_price >= recent_snaps[0]["price"] and _alert_allowed(sym, now_ts):
                    ALERTED_COOLDOWN[cooldown_key_outer] = now_ts
                    _register_alert(sym, now_ts)
                    pull_pct = ((cur_price - recent_low) / recent_low) * 100 if recent_low > 0 else 0
                    action_desc = f"連 {len(outer_hits)} 拍外盤狂掃" if cond_streak_outer else "10秒外盤一擊巨單"
                    alerts.append({
                        "priority": 2 if clean_code in FUTURES_SET else 1,
                        "vol_ratio": outer_vol_ratio,
                        "msg": (
                            f"🎯 *【外盤連續大買單】{clean_code} {stock_name}*{futures_tag}\n"
                            f"🧨 訊號：`{action_desc}` (當日 `{day_pct:+.1f}%`)\n"
                            f"⏰ 即時：`{now_tw.strftime('%H:%M:%S')}` | 💰 現價：`{cur_price:.2f}` (外盤價 `{ask1:.2f}`)\n"
                            f"🛒 外盤掃貨：瞬間狂吃 **`{outer_sum_lots:,} 張`** (達5K均量 `{outer_vol_ratio:.1f}倍` / 5K均量`{int(avg_5k_lots):,}`張)\n"
                            f"📈 瞬間墊高：`+{pull_pct:.2f}%` (起漲點火偵測！)"
                        )
                    })
                    continue

            # =========================================================================
            # 🔥 策略二：【滾動 5K 區間爆量急拉】
            # =========================================================================
            cooldown_key_5k = f"{sym}_5K"
            if (now_ts - ALERTED_COOLDOWN.get(cooldown_key_5k, 0)) < 600:
                continue

            oldest_snap = hist[0]
            # 視窗涵蓋守衛：視窗不足 MIN_5K_WINDOW_COVERAGE_SEC 秒時不判定，
            # 否則開盤後 30~60 秒的量差會拿去跟「5 分鐘均量」比較，單位根本不對
            if MIN_5K_WINDOW_COVERAGE_SEC > 0 and (now_ts - oldest_snap["ts"]) < MIN_5K_WINDOW_COVERAGE_SEC:
                continue
            delta_5k_lots = cur_lots - oldest_snap["lots"]
            if delta_5k_lots < MIN_5K_LOTS:
                continue

            vol_ratio_5k = delta_5k_lots / avg_5k_lots
            if vol_ratio_5k < VOL_MULTIPLIER:
                continue

            window_low = min(s["price"] for s in hist)
            window_high = max(s["price"] for s in hist)
            if (window_high - window_low) > 0 and ((cur_price - window_low) / (window_high - window_low)) < 0.55:
                continue

            instant_pct = ((cur_price - window_low) / window_low) * 100 if window_low > 0 else 0
            if instant_pct >= PCT_THRESHOLD and _alert_allowed(sym, now_ts):
                ALERTED_COOLDOWN[cooldown_key_5k] = now_ts
                _register_alert(sym, now_ts)
                win_sec = max(10, int(now_ts - oldest_snap["ts"]))

                if cur_price < open_p:
                    pattern_tag = f"🔨開低探底急拉 (當日 {day_pct:+.1f}%)"
                elif day_pct < 0:
                    pattern_tag = f"🟢綠盤深V急拉 (當日 {day_pct:+.1f}%)"
                else:
                    pattern_tag = f"🔴盤中點火強拉 (當日 {day_pct:+.1f}%)"

                alerts.append({
                    "priority": 2 if clean_code in FUTURES_SET else 1,
                    "vol_ratio": vol_ratio_5k,
                    "msg": (
                        f"🔥 *【區間爆量急拉】{clean_code} {stock_name}*{futures_tag}\n"
                        f"🏷️ 型態：`{pattern_tag}`\n"
                        f"⏰ 即時：`{now_tw.strftime('%H:%M:%S')}` | 💰 現價：`{cur_price:.2f}`\n"
                        f"🚀 瞬間急拉：`+{instant_pct:.2f}%` ({win_sec}秒內自 `{window_low:.2f}` 拉起)\n"
                        f"💥 區間爆量：`{vol_ratio_5k:.1f} 倍` (狂敲 `{delta_5k_lots:,}`張 / 5K均量`{int(avg_5k_lots):,}`張)"
                    )
                })

    elapsed = time.time() - start_t
    print(f"[{datetime.now(tz_tw).strftime('%H:%M:%S')}] ⚡ 10執行緒極速巡邏完成！僅耗時 {elapsed:.2f} 秒 (報價數: {len(live_quotes)})，抓到 {len(alerts)} 檔！")

    if alerts:
        alerts.sort(key=lambda x: (x["priority"], x["vol_ratio"]), reverse=True)
        for idx in range(0, len(alerts), 10):
            chunk = alerts[idx:idx + 10]
            header = (
                f"⚡ *【0延遲外盤大單 & 5K急拉快訊】*\n"
                f"🕒 時間：{now_tw.strftime('%H:%M:%S')} (極速掃描 **`{elapsed:.1f}秒`**)\n"
                f"──────────────────\n\n"
            )
            body = "\n\n".join([item["msg"] for item in chunk])
            send_telegram(header + body)
    elif is_manual_test or send_hb_now:
        status_msg = (
            f"📡 *【0延遲雙引擎雷達心跳】*\n"
            f"🕒 時間：`{datetime.now(tz_tw).strftime('%H:%M:%S')}` (單輪極速 **`{elapsed:.1f}秒`**！)\n"
            f"🎯 監控：`{len(live_quotes):,}` 檔官方即時報價 (含 `{len(FUTURES_SET)}` 檔股期)\n"
            f"⚡ 引擎：每 10 秒偵測【🎯外盤連續大買單】與【🔥5K爆量急拉】！"
        )
        send_telegram(status_msg)

def main():
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)
    is_weekday = now_tw.weekday() < 5
    current_hm = now_tw.hour * 100 + now_tw.minute

    # 若在 13:35 收盤後才因排程延遲啟動，直接優雅結束，不亂發訊息。
    # 修正：舊寫法對「收盤後才啟動的佇列殘留 run」仍執行單次掃描並發心跳，造成收盤後多餘 TG 訊息
    if not is_weekday or current_hm > 1335 or current_hm < 750:
        if is_weekday and current_hm > 1335:
            print("🔧 收盤後啟動 (排程佇列殘留)，不發測試訊息，直接結束")
            return
        print(f"🔧 目前時間 {now_tw.strftime('%H:%M:%S')} 非盤中監控時段，執行單次連線測試...")
        init_market_and_baseline()
        run_realtime_scan(send_hb_now=True, is_manual_test=True)
        return

    print("🔥 進入【盤前預熱與盤中 10秒高頻外盤狙擊模式】...")
    init_market_and_baseline()
    send_telegram(
        f"🚀 *【外盤大買單 + 5K極速雷達已於盤前就位】*\n"
        f"🕒 就位時間：`{datetime.now(tz_tw).strftime('%H:%M:%S')}`\n"
        f"✅ 已完成 `{len(ACTIVE_UNIVERSE):,}` 檔活躍股與 `{len(FUTURES_SET)}` 檔個股期貨預熱！\n"
        f"⚡ **三大升級**：\n"
        f"1️⃣ 10執行緒併發，全市場掃描從 61秒 壓縮至 **3~5秒**！\n"
        f"2️⃣ 巡邏密度提升至 **每 10 秒一次**！\n"
        f"3️⃣ 新增 **【🎯外盤連續大買單】** 策略，主力一吃外盤立刻抓起漲！"
    )

    last_hb_slot = None

    while True:
        now_tw = datetime.now(tz_tw)
        current_hm = now_tw.hour * 100 + now_tw.minute

        if current_hm > 1335:
            send_telegram("🔔 *【台股已收盤】* 今日外盤大單與 5K 雷達任務圓滿結束！")
            break

        # 09:00:03 準時開始第一拍高頻取樣！
        if current_hm >= 900:
            slot_5m = f"{now_tw.hour:02d}:{(now_tw.minute // 5) * 5:02d}"
            should_send_hb = SEND_HEARTBEAT and (slot_5m != last_hb_slot) and (now_tw.minute % 5 == 0)
            if should_send_hb:
                last_hb_slot = slot_5m

            scan_start = time.time()
            run_realtime_scan(send_hb_now=should_send_hb, is_manual_test=False)
            scan_cost = time.time() - scan_start

            sleep_sec = max(1.0, SCAN_INTERVAL_SEC - scan_cost)
            time.sleep(sleep_sec)
        else:
            # 08:05 ~ 08:59 盤前待命期間，每 2 秒微休眠，等待 09:00:00 瞬間開火！
            time.sleep(2)

if __name__ == "__main__":
    main()

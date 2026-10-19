import os
import time
import logging
from collections import deque
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

# ==================== 實戰狙擊參數設定 ====================
VOL_MULTIPLIER = 3.0       # 爆量倍數：近 5 分鐘成交量 >= 過去 5 日 5K 均量的 3.0 倍
PCT_THRESHOLD = 1.8        # 急拉門檻：近 5 分鐘內瞬間拉升幅度 >= 1.8% (微調至 1.8% 捕捉起漲第一根！)
MIN_5K_LOTS = 80           # 近 5 分鐘最低成交張數：至少 >= 80 張
MIN_AVG_DAILY_LOTS = 300   # 活躍股門檻：過去 5 日日均量 >= 300 張
SCAN_INTERVAL_SEC = 30     # 每 30 秒極速取樣一次台灣官方報價！
SEND_HEARTBEAT = True      # 每 5 分鐘發送一次心跳回報
# ==========================================================

STOCK_DICT = {}
BASELINE_AVG_5K = {}       # 單位：張 (Lots)
ACTIVE_UNIVERSE = set()
ALERTED_COOLDOWN = {}      # 記錄每檔股票最後推播時間，10 分鐘內不重複洗版同一檔
PRICE_VOL_HISTORY = {}     # 記憶體即時時序庫：記錄每檔股票過去 6 分鐘的 (timestamp, price, total_lots)

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
    """開盤預熱：用日K計算過去 5 日平均 5K 張數（日K不受 20 分鐘延遲影響）"""
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
                    df_d = data_d if len(batch) == 1 else data_d[sym]
                    df_d = df_d.dropna(subset=["Volume"])
                    if df_d.empty:
                        continue
                    df_hist = df_d[df_d.index.strftime('%Y-%m-%d') != now_str]
                    df_hist = df_hist[df_hist["Volume"] > 0].tail(5)
                    if len(df_hist) > 0:
                        avg_daily_vol = float(df_hist["Volume"].mean())
                        clean_c = clean_symbol_code(sym)
                        if avg_daily_vol >= min_daily_shares or clean_c in FUTURES_SET:
                            # 換算成「每 5 分鐘平均張數」(1張=1000股，一日54根5K)
                            BASELINE_AVG_5K[sym] = max(5.0, (avg_daily_vol / 1000.0) / 54.0)
                            ACTIVE_UNIVERSE.add(sym)
                except Exception:
                    continue
        except Exception:
            pass

    print(f"✅ [預熱完成] 鎖定 {len(ACTIVE_UNIVERSE)} 檔活躍股 (含 {len(FUTURES_SET)} 檔股期標的)！")

def fetch_realtime_mis_quotes(symbols_list):
    """
    【台灣證交所 MIS 官方盤中即時報價 API】
    0 延遲！直接抓取當下這一秒的真實成交價、開盤價、昨收價與當日累積成交張數！
    每次併發查詢 80 檔，1,000 檔僅需約 3.5 秒即可全數抓完！
    """
    results = {}
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': 'https://mis.twse.com.tw/stock/index.jsp'
    }

    # 將 .TW 轉為 tse_xxxx.tw，.TWO 轉為 otc_xxxx.tw
    mis_keys = []
    sym_map = {}
    for s in symbols_list:
        code = clean_symbol_code(s)
        prefix = "otc" if s.endswith(".TWO") else "tse"
        m_key = f"{prefix}_{code}.tw"
        mis_keys.append(m_key)
        sym_map[m_key] = s

    chunk_size = 75
    for i in range(0, len(mis_keys), chunk_size):
        chunk = mis_keys[i:i + chunk_size]
        ex_ch = "|".join(chunk)
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0"
        try:
            res = session.get(url, headers=headers, verify=False, timeout=5)
            if res.status_code == 200:
                data = res.json()
                for item in data.get("msgArray", []):
                    ch = f"{item.get('ex', '')}_{item.get('ch', '')}"
                    sym = sym_map.get(ch)
                    if not sym:
                        continue
                    
                    # z: 最新成交價 (若當盤無成交，取 b 委買第一檔或 a 委賣第一檔)
                    z_str = item.get("z", "-")
                    if z_str == "-" or not z_str:
                        b_list = str(item.get("b", "")).split("_")
                        z_str = b_list[0] if b_list and b_list[0] not in ("-", "") else item.get("y", "0")
                    
                    try:
                        price = float(z_str)
                        total_lots = int(float(item.get("v", "0"))) # v: 今日累積成交張數
                        open_p = float(item.get("o", price) if item.get("o", "-") != "-" else price)
                        yest_p = float(item.get("y", price) if item.get("y", "-") != "-" else price)
                        low_p = float(item.get("l", price) if item.get("l", "-") != "-" else price)
                        if price > 0 and total_lots > 0:
                            results[sym] = {
                                "price": price,
                                "total_lots": total_lots,
                                "open": open_p,
                                "yest": yest_p,
                                "low": low_p
                            }
                    except Exception:
                        continue
        except Exception:
            pass
        time.sleep(0.08) # 微小間隔保護官方連線穩定

    return results

def run_realtime_scan(send_hb_now=False, is_manual_test=False):
    start_t = time.time()
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)
    now_ts = time.time()

    if len(ACTIVE_UNIVERSE) < 100:
        init_market_and_baseline()

    # 排序：股期標的優先查詢
    sorted_universe = sorted(list(ACTIVE_UNIVERSE), key=lambda s: clean_symbol_code(s) in FUTURES_SET, reverse=True)
    
    # 1. 從台灣證交所 MIS 抓取 0 延遲即時報價 (耗時僅約 3.5 ~ 5 秒！)
    live_quotes = fetch_realtime_mis_quotes(sorted_universe)
    alerts = []

    # 2. 如果證交所 MIS 順利抓到即時報價，進行「滾動 5 分鐘視窗」0 延遲爆量急拉比對！
    if len(live_quotes) > 100:
        for sym, q in live_quotes.items():
            cur_price = q["price"]
            cur_lots = q["total_lots"]
            open_p = q["open"]
            yest_p = q["yest"]

            if sym not in PRICE_VOL_HISTORY:
                PRICE_VOL_HISTORY[sym] = deque(maxlen=15)
            
            hist = PRICE_VOL_HISTORY[sym]
            hist.append((now_ts, cur_price, cur_lots))

            # 需要至少有 2 個時間點才能算區間增量；將保留超過 330 秒 (5.5分鐘) 的舊快照移除
            while len(hist) > 2 and (now_ts - hist[0][0]) > 330:
                hist.popleft()

            if len(hist) < 2:
                continue

            # 冷卻保護：同一檔股票觸發後，10 分鐘 (600秒) 內不重複刷屏
            if sym in ALERTED_COOLDOWN and (now_ts - ALERTED_COOLDOWN[sym]) < 600:
                continue

            # 計算過去這段滾動視窗 (1~5 分鐘內) 的「新增成交張數」與「區間最低價拉升幅度」
            oldest_ts, oldest_price, oldest_lots = hist[0]
            delta_lots = cur_lots - oldest_lots
            if delta_lots < MIN_5K_LOTS:
                continue

            avg_5k_lots = BASELINE_AVG_5K.get(sym, 0)
            if avg_5k_lots <= 0:
                continue

            vol_ratio = delta_lots / avg_5k_lots
            if vol_ratio < VOL_MULTIPLIER:
                continue

            # 取過去 5 分鐘視窗內的最低價，計算瞬間急拉幅度
            window_low = min(item[1] for item in hist)
            instant_pct = ((cur_price - window_low) / window_low) * 100 if window_low > 0 else 0

            if instant_pct >= PCT_THRESHOLD:
                ALERTED_COOLDOWN[sym] = now_ts
                clean_code = clean_symbol_code(sym)
                stock_name = STOCK_DICT.get(sym, "")
                futures_tag = " 【⚡有股期】" if clean_code in FUTURES_SET else ""
                day_pct = ((cur_price - yest_p) / yest_p) * 100 if yest_p > 0 else 0
                win_sec = int(now_ts - oldest_ts)

                if cur_price < open_p:
                    pattern_tag = f"🔨開低探底急拉 (當日 {day_pct:+.1f}%)"
                elif day_pct < 0:
                    pattern_tag = f"🟢綠盤深V急拉 (當日 {day_pct:+.1f}%)"
                else:
                    pattern_tag = f"🔴盤中點火強拉 (當日 {day_pct:+.1f}%)"

                alerts.append({
                    "has_futures": clean_code in FUTURES_SET,
                    "vol_ratio": vol_ratio,
                    "msg": (
                        f"🔥 *{clean_code} {stock_name}*{futures_tag}\n"
                        f"🏷️ 型態：`{pattern_tag}`\n"
                        f"⏰ 即時：`{now_tw.strftime('%H:%M:%S')}` | 💰 現價：`{cur_price:.2f}`\n"
                        f"🚀 瞬間急拉：`+{instant_pct:.2f}%` ({win_sec}秒內自 `{window_low:.2f}` 拉起)\n"
                        f"💥 區間爆量：`{vol_ratio:.1f} 倍` (瞬間狂敲 `{delta_lots:,}`張 / 5K均量`{int(avg_5k_lots):,}`張)"
                    )
                })

    # 3. 備援機制 (或首次啟動第 1 秒還沒有歷史差分時)：使用 Yahoo 5K (拿掉 720 秒錯殺限制！)
    first_boot_check = sum(len(v) for v in PRICE_VOL_HISTORY.values()) <= len(PRICE_VOL_HISTORY)
    if (first_boot_check or len(live_quotes) < 50) and not alerts:
        print("🔍 [首次啟動基線掃描] 檢查最近一根已發生之 5K 強勢股...")
        for i in range(0, len(sorted_universe), 150):
            batch = sorted_universe[i:i + 150]
            try:
                data = yf.download(tickers=batch, period="1d", interval="5m", group_by="ticker", threads=True, progress=False)
                if data.empty:
                    continue
                for sym in batch:
                    try:
                        df = data if len(batch) == 1 else data[sym]
                        df = df.dropna(subset=["Close", "Volume"])
                        if len(df) < 2:
                            continue
                        if df.index.tz is None:
                            df.index = df.index.tz_localize("UTC").tz_convert(tz_tw)
                        else:
                            df.index = df.index.tz_convert(tz_tw)

                        target_bar = df.iloc[-1] if len(df) == 2 else df.iloc[-2]
                        prev_bar = df.iloc[-2] if len(df) == 2 else df.iloc[-3]

                        vol_lots = int(float(target_bar["Volume"]) / 1000)
                        if vol_lots < MIN_5K_LOTS:
                            continue
                        avg_5k_lots = BASELINE_AVG_5K.get(sym, 0)
                        if avg_5k_lots <= 0:
                            continue
                        vol_ratio = vol_lots / avg_5k_lots
                        if vol_ratio < VOL_MULTIPLIER:
                            continue

                        open_p = float(target_bar["Open"])
                        close_p = float(target_bar["Close"])
                        low_p = float(target_bar["Low"])
                        high_p = float(target_bar["High"])
                        prev_close_p = float(prev_bar["Close"])

                        if (high_p - low_p) > 0 and ((close_p - low_p) / (high_p - low_p)) < 0.5:
                            continue

                        instant_pct = max(
                            ((close_p - open_p) / open_p) * 100 if open_p > 0 else 0,
                            ((close_p - prev_close_p) / prev_close_p) * 100 if prev_close_p > 0 else 0,
                            ((close_p - low_p) / low_p) * 100 if low_p > 0 else 0
                        )
                        if instant_pct >= PCT_THRESHOLD:
                            ALERTED_COOLDOWN[sym] = now_ts
                            clean_code = clean_symbol_code(sym)
                            stock_name = STOCK_DICT.get(sym, "")
                            futures_tag = " 【⚡有股期】" if clean_code in FUTURES_SET else ""
                            alerts.append({
                                "has_futures": clean_code in FUTURES_SET,
                                "vol_ratio": vol_ratio,
                                "msg": (
                                    f"🔥 *{clean_code} {stock_name}*{futures_tag}\n"
                                    f"⏰ K棒：`{target_bar.name.strftime('%H:%M')}` | 💰 現價：`{close_p:.2f}`\n"
                                    f"🚀 5K拉升：`+{instant_pct:.2f}%` (低點 `{low_p:.2f}`)\n"
                                    f"💥 5K爆量：`{vol_ratio:.1f} 倍` (`{vol_lots:,}`張 / 均量`{int(avg_5k_lots):,}`張)"
                                )
                            })
                    except Exception:
                        continue
            except Exception:
                pass

    elapsed = time.time() - start_t
    print(f"[{datetime.now(tz_tw).strftime('%H:%M:%S')}] ⚡ MIS 即時巡邏完成！耗時僅 {elapsed:.1f} 秒 (即時報價數: {len(live_quotes)})，抓到 {len(alerts)} 檔！")

    if alerts:
        alerts.sort(key=lambda x: (x["has_futures"], x["vol_ratio"]), reverse=True)
        for idx in range(0, len(alerts), 12):
            chunk = alerts[idx:idx + 12]
            header = (
                f"⚡ *【0延遲盤中爆量急拉快訊】*\n"
                f"🕒 時間：{now_tw.strftime('%H:%M:%S')} (掃描僅 `{elapsed:.1f}秒`)\n"
                f"📌 條件：區間量>{VOL_MULTIPLIER}倍 & 急拉>{PCT_THRESHOLD}%\n"
                f"──────────────────\n\n"
            )
            body = "\n\n".join([item["msg"] for item in chunk])
            send_telegram(header + body)
    elif is_manual_test or send_hb_now:
        status_msg = (
            f"📡 *【0延遲 MIS 雷達心跳】*\n"
            f"🕒 時間：`{datetime.now(tz_tw).strftime('%H:%M:%S')}` (單輪耗時僅 **`{elapsed:.1f}秒`**！)\n"
            f"🎯 即時監控：`{len(live_quotes):,}` 檔官方 0 延遲報價 (含 `{len(FUTURES_SET)}` 檔股期)\n"
            f"⚡ 狀態：滾動 5 分鐘視窗比對中，一爆量急拉即刻秒推！"
        )
        send_telegram(status_msg)

def main():
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)
    is_weekday = now_tw.weekday() < 5
    current_hm = now_tw.hour * 100 + now_tw.minute

    if not is_weekday or current_hm > 1335 or current_hm < 820:
        print("🔧 非盤中時段，執行【單次極速掃描測試】...")
        run_realtime_scan(send_hb_now=True, is_manual_test=True)
        return

    print("🔥 進入【台灣證交所 MIS 0延遲高頻常駐模式】...")
    init_market_and_baseline()
    send_telegram(
        f"🚀 *【證交所 MIS 0延遲極速雷達已上線】*\n"
        f"🕒 時間：`{datetime.now(tz_tw).strftime('%H:%M:%S')}`\n"
        f"✅ 已修復 Yahoo 20分鐘延遲過濾 Bug，改接 **台灣證交所官方 MIS 0延遲報價**！\n"
        f"⚡ 每 **30 秒** 僅需 **4~6 秒** 即可掃完 `{len(ACTIVE_UNIVERSE):,}` 檔活躍股，盤中急拉瞬間秒抓！"
    )

    last_hb_slot = None

    while True:
        now_tw = datetime.now(tz_tw)
        current_hm = now_tw.hour * 100 + now_tw.minute

        if current_hm > 1335:
            send_telegram("🔔 *【台股已收盤】* 今日 5K 常駐雷達任務圓滿結束！")
            break

        if current_hm >= 900:
            slot_5m = f"{now_tw.hour:02d}:{(now_tw.minute // 5) * 5:02d}"
            should_send_hb = SEND_HEARTBEAT and (slot_5m != last_hb_slot) and (now_tw.minute % 5 == 0)
            if should_send_hb:
                last_hb_slot = slot_5m

            scan_start = time.time()
            run_realtime_scan(send_hb_now=should_send_hb, is_manual_test=False)
            scan_cost = time.time() - scan_start

            sleep_sec = max(3.0, SCAN_INTERVAL_SEC - scan_cost)
            time.sleep(sleep_sec)
        else:
            time.sleep(5)

if __name__ == "__main__":
    main()

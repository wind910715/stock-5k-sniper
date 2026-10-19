import os
import time
import logging
from datetime import datetime
import pytz
import requests
import urllib3
import pandas as pd
import yfinance as yf

# 徹底關閉 yfinance 的 Failed download 刷屏報錯，大幅提升 I/O 速度！
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ==================== 極速實戰參數設定 ====================
VOL_MULTIPLIER = 3.0       # 爆量倍數：單根 5K 量 >= 過去 5 日 5K 均量的 3.0 倍
PCT_THRESHOLD = 2.0        # 急拉門檻：單根 5K 瞬間拉升幅度 >= 2.0%
MIN_5K_LOTS = 100          # 單根 5K 最低張數：至少 >= 100 張
MIN_AVG_DAILY_LOTS = 400   # 活躍股池門檻：過去 5 日均量 >= 400 張 (精準鎖定約 750 檔主力活躍股)
BATCH_SIZE = 150           # 極速併發批次大小
SCAN_INTERVAL_SEC = 60     # 每 60 秒 (1分鐘) 巡邏一次！盤中一達標立刻秒推，不等 5 分鐘收線！
SEND_HEARTBEAT = True      # 每 5 分鐘整點發送一次極簡心跳回報
# ==========================================================

STOCK_DICT = {}
BASELINE_AVG_5K = {}
ACTIVE_UNIVERSE = []
BLACKLIST_SYMS = set()     # 自動記憶無效代號，永不重複下載
ALERTED_RECORDS = set()    # 防止同一根 5K 重複推播

# 內建台灣期交所核心「個股期貨」標的庫 + 動態爬蟲雙保險 (解決 0 檔股期問題)
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
    """正確移除 .TWO 與 .TW，不再殘留英文字母 O"""
    if sym.endswith(".TWO"):
        return sym[:-4]
    if sym.endswith(".TW"):
        return sym[:-3]
    return sym

def is_valid_stock_code(code):
    """過濾掉美債 ETF (B結尾)、外幣 ETF 與權證，徹底消滅 Failed download"""
    if not (4 <= len(code) <= 6):
        return False
    # 排除 00xxxB (債券ETF，Yahoo無報價且盤中無波動)、00xxxU、00xxxR
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

    # 補充抓取期交所最新股期清單
    try:
        res_sf = requests.get("https://openapi.taifex.com.tw/v1/SingleStockFuturesMargining", headers=headers, verify=False, timeout=6)
        if res_sf.status_code == 200:
            for row in res_sf.json():
                u_code = str(row.get("UnderlyingSecurityCode", "")).strip()
                if len(u_code) >= 4:
                    FUTURES_SET.add(u_code)
    except Exception:
        pass

    # 載入上市與上櫃清單 (排除債券 ETF 與異常代號)
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
    print(f"📥 [預熱] 載入 {len(all_symbols)} 檔有效標的，開始計算 5 日均量並剃除無效代號...")

    temp_universe = []
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
                        BLACKLIST_SYMS.add(sym)
                        continue
                    df_hist = df_d[df_d.index.strftime('%Y-%m-%d') != now_str]
                    df_hist = df_hist[df_hist["Volume"] > 0].tail(5)
                    if len(df_hist) > 0:
                        avg_daily_vol = float(df_hist["Volume"].mean())
                        clean_c = clean_symbol_code(sym)
                        if avg_daily_vol >= min_daily_shares or clean_c in FUTURES_SET:
                            BASELINE_AVG_5K[sym] = max(1000.0, avg_daily_vol / 54.0)
                            temp_universe.append(sym)
                except Exception:
                    BLACKLIST_SYMS.add(sym)
        except Exception:
            pass

    # 將有股期的排在最前面優先掃描！
    temp_universe.sort(key=lambda s: clean_symbol_code(s) in FUTURES_SET, reverse=True)
    ACTIVE_UNIVERSE = temp_universe
    print(f"✅ [預熱完成] 鎖定 {len(ACTIVE_UNIVERSE)} 檔高速活躍池 (含 {len(FUTURES_SET)} 檔股期庫)，已剔除 {len(BLACKLIST_SYMS)} 檔無效代號！")

def run_single_scan(is_manual_test=False, send_hb_now=False):
    start_t = time.time()
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)

    if len(ACTIVE_UNIVERSE) < 100:
        init_market_and_baseline()

    valid_pool = [s for s in ACTIVE_UNIVERSE if s not in BLACKLIST_SYMS]
    total_alerts_count = 0

    for i in range(0, len(valid_pool), BATCH_SIZE):
        batch = valid_pool[i:i + BATCH_SIZE]
        batch_alerts = []
        try:
            data = yf.download(
                tickers=batch,
                period="1d",
                interval="5m",
                group_by="ticker",
                threads=True,
                progress=False
            )
            if data.empty:
                continue

            for sym in batch:
                try:
                    df = data if len(batch) == 1 else data[sym]
                    df = df.dropna(subset=["Close", "Volume"])
                    if df.empty:
                        BLACKLIST_SYMS.add(sym)
                        continue
                    if len(df) < 2:
                        continue

                    if df.index.tz is None:
                        df.index = df.index.tz_localize("UTC").tz_convert(tz_tw)
                    else:
                        df.index = df.index.tz_convert(tz_tw)

                    # 【重大加速改革】直接檢查最新一根正在跳動的 5K (iloc[-1]) 與剛收線的 5K (iloc[-2])！
                    # 只要盤中當下這一秒爆量突破 3 倍且拉升超過 2%，不等 5 分鐘收完，立刻第 1 秒警報！
                    bars_to_check = [df.iloc[-1]]
                    if len(df) >= 3:
                        bars_to_check.append(df.iloc[-2])

                    day_open_p = float(df["Open"].iloc[0])
                    avg_5k_vol = BASELINE_AVG_5K.get(sym, 0)
                    if avg_5k_vol <= 0:
                        continue

                    for idx_pos, target_bar in enumerate(bars_to_check):
                        # 過濾掉延遲超過 12 分鐘的過期舊 K 棒 (盤中防延遲保護)
                        bar_age_sec = (now_tw - target_bar.name).total_seconds()
                        if not is_manual_test and bar_age_sec > 720:
                            continue

                        bar_time_str = target_bar.name.strftime("%H:%M")
                        record_key = f"{sym}_{bar_time_str}"
                        if not is_manual_test and record_key in ALERTED_RECORDS:
                            continue

                        current_5k_vol = float(target_bar["Volume"])
                        vol_lots = int(current_5k_vol / 1000)
                        if vol_lots < MIN_5K_LOTS:
                            continue

                        vol_ratio = current_5k_vol / avg_5k_vol
                        if vol_ratio < VOL_MULTIPLIER:
                            continue

                        # 找出前一根 K 棒收盤價
                        prev_bar = df.iloc[-2] if idx_pos == 0 else df.iloc[-3]
                        open_p = float(target_bar["Open"])
                        close_p = float(target_bar["Close"])
                        low_p = float(target_bar["Low"])
                        high_p = float(target_bar["High"])
                        prev_close_p = float(prev_bar["Close"])

                        body_pct = ((close_p - open_p) / open_p) * 100 if open_p > 0 else 0
                        jump_pct = ((close_p - prev_close_p) / prev_close_p) * 100 if prev_close_p > 0 else 0
                        rebound_pct = ((close_p - low_p) / low_p) * 100 if low_p > 0 else 0

                        bar_range = high_p - low_p
                        close_pos = (close_p - low_p) / bar_range if bar_range > 0 else 1.0
                        if close_pos < 0.5:
                            continue

                        instant_pct = max(body_pct, jump_pct, rebound_pct)

                        if instant_pct >= PCT_THRESHOLD:
                            ALERTED_RECORDS.add(record_key)
                            clean_code = clean_symbol_code(sym)
                            stock_name = STOCK_DICT.get(sym, "")
                            avg_lots = max(1, int(avg_5k_vol / 1000))
                            futures_tag = " 【⚡有股期】" if clean_code in FUTURES_SET else ""

                            if close_p < open_p:
                                pattern_tag = "🔨收黑長下影探底"
                            elif close_p < day_open_p:
                                pattern_tag = "🟢低檔深V急拉"
                            else:
                                pattern_tag = "🔴強勢點火突破"

                            batch_alerts.append({
                                "has_futures": clean_code in FUTURES_SET,
                                "vol_ratio": vol_ratio,
                                "msg": (
                                    f"🔥 *{clean_code} {stock_name}*{futures_tag}\n"
                                    f"🏷️ 型態：`{pattern_tag}`\n"
                                    f"⏰ K棒：`{bar_time_str}` | 💰 現價：`{close_p:.2f}`\n"
                                    f"🚀 5K拉升：`+{instant_pct:.2f}%` (低點 `{low_p:.2f}`)\n"
                                    f"💥 5K爆量：`{vol_ratio:.1f} 倍` (`{vol_lots:,}`張 / 均量`{avg_lots:,}`張)"
                                )
                            })
                            break
                except Exception:
                    continue

            # 【即掃即推 Streaming Push】這一批只要抓到強勢股，不等後面批次，第 1 秒立刻發送 TG！
            if batch_alerts:
                total_alerts_count += len(batch_alerts)
                batch_alerts.sort(key=lambda x: (x["has_futures"], x["vol_ratio"]), reverse=True)
                now_push_str = datetime.now(tz_tw).strftime('%H:%M:%S')
                header = f"⚡ *【5K 即時點火快訊】* ({now_push_str})\n──────────────────\n"
                body = "\n\n".join([item["msg"] for item in batch_alerts[:12]])
                send_telegram(header + body)

        except Exception:
            pass

    elapsed = time.time() - start_t
    print(f"[{datetime.now(tz_tw).strftime('%H:%M:%S')}] 🏁 巡邏完成！耗時 {elapsed:.1f} 秒，觸發 {total_alerts_count} 檔。")

    if total_alerts_count == 0 and (is_manual_test or send_hb_now):
        status_msg = (
            f"📡 *【5K 極速雷達心跳】*\n"
            f"🕒 時間：`{datetime.now(tz_tw).strftime('%H:%M:%S')}` (單輪耗時僅 `{elapsed:.1f}秒`)\n"
            f"🎯 精準池：`{len(valid_pool):,}` 檔活躍股 (含 `{len(FUTURES_SET)}` 檔股期標的)\n"
            f"⚡ 模式：每 60 秒即時監控正在跳動之最新 5K (即掃即推)"
        )
        send_telegram(status_msg)

def main():
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)
    is_weekday = now_tw.weekday() < 5
    current_hm = now_tw.hour * 100 + now_tw.minute

    if not is_weekday or current_hm > 1335 or current_hm < 820:
        print("🔧 非盤中時段，執行【單次極速掃描測試】...")
        run_single_scan(is_manual_test=True, send_hb_now=True)
        return

    print("🔥 進入【盤中每分鐘即時點火常駐模式】...")
    init_market_and_baseline()
    send_telegram(
        f"🚀 *【5K 極速即掃即推雷達已上線】*\n"
        f"🕒 時間：`{datetime.now(tz_tw).strftime('%H:%M:%S')}`\n"
        f"✅ 高速池：`{len(ACTIVE_UNIVERSE):,}` 檔 (已剔除無效債券ETF，內建 `{len(FUTURES_SET)}` 檔股期)\n"
        f"⚡ 升級：每 **60 秒** 巡邏一次正在跳動的最新 5K，一爆量達標 **3 秒內即掃即推**！"
    )

    last_hb_slot = None

    while True:
        now_tw = datetime.now(tz_tw)
        current_hm = now_tw.hour * 100 + now_tw.minute

        if current_hm > 1335:
            send_telegram("🔔 *【台股已收盤】* 今日 5K 常駐雷達任務圓滿結束！")
            break

        if current_hm >= 900:
            # 每 5 分鐘整點發送一次心跳，但每 1 分鐘都會在背景高速抓爆量股！
            slot_5m = f"{now_tw.hour:02d}:{(now_tw.minute // 5) * 5:02d}"
            should_send_hb = SEND_HEARTBEAT and (slot_5m != last_hb_slot) and (now_tw.minute % 5 == 0)
            if should_send_hb:
                last_hb_slot = slot_5m

            scan_start = time.time()
            run_single_scan(is_manual_test=False, send_hb_now=should_send_hb)
            scan_cost = time.time() - scan_start

            # 扣除掃描耗時，確保每 60 秒密集巡邏一次！
            sleep_sec = max(5.0, SCAN_INTERVAL_SEC - scan_cost)
            time.sleep(sleep_sec)
        else:
            time.sleep(5)

if __name__ == "__main__":
    main()

import os
import time
from datetime import datetime
import pytz
import requests
import urllib3
import pandas as pd
import yfinance as yf

# 隱藏 SSL 與 Pandas 警告，保持 Log 乾淨
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ==================== 實戰策略參數設定 ====================
VOL_MULTIPLIER = 3.0       # 爆量倍數：單根 5K 量 >= 過去 5 日 5K 均量的 3.0 倍
PCT_THRESHOLD = 2.0        # 急拉門檻：單根 5K 瞬間拉升幅度 >= 2.0%
MIN_5K_LOTS = 100          # 單根 5K 最低張數：至少 >= 100 張 (過濾冷門小型股雜訊)
MIN_AVG_DAILY_LOTS = 250   # 活躍股池門檻：過去 5 日平均日量 >= 250 張才納入盤中高速池
BATCH_SIZE = 180           # 每批併發下載檔數 (180 檔兼顧極速與穩定)
SEND_HEARTBEAT = True      # 是否每 5 分鐘發送保底心跳回報 (若覺得無訊號時太吵，可改為 False)
# ==========================================================

STOCK_DICT = {}            # {'2330.TW': '台積電', ...}
FUTURES_SET = set()        # 擁有「個股期貨」的代號集合 {'2330', '2454', ...}
BASELINE_AVG_5K = {}       # 快取：過去 5 日 5K 均量 (股數)
ACTIVE_UNIVERSE = []       # 盤中高速監控池
ALERTED_RECORDS = set()    # 已推播過的 (股票_K棒時間)，防止重複推播

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ 未設定 TELEGRAM_TOKEN 或 TELEGRAM_CHAT_ID")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    for attempt in range(3):
        try:
            res = requests.post(url, json=payload, timeout=12)
            if res.status_code == 200:
                return
            # 若 Markdown 特殊字元解析錯誤，改用純文字重發一次確保必達
            if res.status_code == 400:
                payload.pop("parse_mode", None)
                requests.post(url, json=payload, timeout=12)
                return
            print(f"⚠️ TG 發送重試 ({attempt+1}/3): {res.text}")
        except Exception as e:
            print(f"⚠️ TG 連線異常 ({attempt+1}/3): {e}")
        time.sleep(2)

def init_market_and_baseline():
    """
    【開盤前一次性預熱】
    載入全市場上市櫃個股、主動/被動 ETF、期交所個股期貨清單，
    並預先算好「過去 5 日 5K 均量」存入記憶體，讓盤中每 5 分鐘掃描只需抓 1d 資料！
    """
    global STOCK_DICT, FUTURES_SET, BASELINE_AVG_5K, ACTIVE_UNIVERSE
    if len(ACTIVE_UNIVERSE) > 300:
        return

    tz_tw = pytz.timezone("Asia/Taipei")
    now_str = datetime.now(tz_tw).strftime('%Y-%m-%d')
    print("📥 [1/3] 正在載入全市場股票名稱與期交所【個股期貨】清單...")
    headers = {'User-Agent': 'Mozilla/5.0'}

    # 1. 載入台灣期交所「股票期貨標的」
    try:
        res_sf = requests.get("https://www.taifex.com.tw/cht/2/stockLists", headers=headers, verify=False, timeout=10)
        for df in pd.read_html(res_sf.text):
            for col in df.columns:
                if "證券代號" in str(col) or "標的" in str(col):
                    for val in df[col].dropna():
                        code = str(val).strip().split()[0]
                        if 4 <= len(code) <= 6:
                            FUTURES_SET.add(code)
    except Exception as e:
        print(f"⚠️ 期交所清單略過: {e}")

    # 2. 載入上市 (TWSE) 與上櫃 (TPEx) 代碼與中文名稱
    try:
        r1 = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", headers=headers, verify=False, timeout=10)
        if r1.status_code == 200:
            for row in r1.json():
                code = row.get("Code", "").strip()
                if 4 <= len(code) <= 6:
                    STOCK_DICT[f"{code}.TW"] = row.get("Name", "").strip()
    except Exception:
        pass

    try:
        r2 = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", headers=headers, verify=False, timeout=10)
        if r2.status_code == 200:
            for row in r2.json():
                code = row.get("SecuritiesCompanyCode", "").strip()
                if 4 <= len(code) <= 6:
                    STOCK_DICT[f"{code}.TWO"] = row.get("CompanyName", "").strip()
    except Exception:
        pass

    # 備援：若 OpenAPI 維護，從證交所 ISIN 總表抓取 (完整涵蓋 00980A/00981A 等新 ETF)
    if len(STOCK_DICT) < 1500:
        for mode, suffix in [('2', '.TW'), ('4', '.TWO')]:
            try:
                res = requests.get(f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}", headers=headers, verify=False, timeout=15)
                res.encoding = 'big5'
                df = pd.read_html(res.text)[0]
                df.columns = df.iloc[0]
                df = df.iloc[1:]
                cond = (df['CFICode'] == 'ESVUFR') | (df['CFICode'].astype(str).str.startswith('CEO'))
                for item in df[cond]['有價證券代號及名稱']:
                    parts = str(item).replace('\u3000', ' ').split()
                    if len(parts) >= 2 and (4 <= len(parts[0].strip()) <= 6):
                        STOCK_DICT[f"{parts[0].strip()}{suffix}"] = parts[1].strip()
            except Exception:
                pass

    all_symbols = list(STOCK_DICT.keys())
    print(f"📥 [2/3] 成功取得全市場 {len(all_symbols)} 檔標的，正在建立【過去5日均量快取】...")

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
                        continue
                    # 排除今天當日未收盤的日K，只取過去完整 5 個交易日
                    df_hist = df_d[df_d.index.strftime('%Y-%m-%d') != now_str]
                    df_hist = df_hist[df_hist["Volume"] > 0].tail(5)
                    if len(df_hist) > 0:
                        avg_daily_vol = float(df_hist["Volume"].mean())
                        clean_c = sym.replace(".TW", "").replace(".TWO", "")
                        # 只要過去 5 日均量 >= 250 張，或是有發行「個股期貨」的標的，全部納入高速監控池！
                        if avg_daily_vol >= min_daily_shares or clean_c in FUTURES_SET:
                            BASELINE_AVG_5K[sym] = max(1000.0, avg_daily_vol / 54.0)
                            temp_universe.append(sym)
                except Exception:
                    continue
        except Exception as e:
            print(f"⚠️ 預熱批次異常: {e}")

    ACTIVE_UNIVERSE = temp_universe
    print(f"✅ [3/3] 預熱完成！鎖定 {len(ACTIVE_UNIVERSE)} 檔活躍股與股期標的 (含 {len(FUTURES_SET)} 檔期貨標的)！")

def run_single_scan(is_manual_test=False):
    start_t = time.time()
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)

    # 若快取尚未建立，自動執行預熱
    if len(ACTIVE_UNIVERSE) < 300:
        init_market_and_baseline()

    if not ACTIVE_UNIVERSE:
        print("❌ 無法取得活躍股池，略過本次掃描。")
        return

    print(f"[{now_tw.strftime('%H:%M:%S')}] ⚡ 開始極速掃描 {len(ACTIVE_UNIVERSE)} 檔盤中 5K...")
    alerts = []

    for i in range(0, len(ACTIVE_UNIVERSE), BATCH_SIZE):
        batch = ACTIVE_UNIVERSE[i:i + BATCH_SIZE]
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
                    if len(df) < 2:
                        continue

                    if df.index.tz is None:
                        df.index = df.index.tz_localize("UTC").tz_convert(tz_tw)
                    else:
                        df.index = df.index.tz_convert(tz_tw)

                    # 【未收線保護】若最新一根 5K 產生還不到 4 分 05 秒 (245秒)，取上一根已完整收線的 5K
                    last_bar_time = df.index[-1]
                    if (now_tw - last_bar_time).total_seconds() < 245 and len(df) >= 3:
                        target_bar = df.iloc[-2]
                        prev_bar = df.iloc[-3]
                    else:
                        target_bar = df.iloc[-1]
                        prev_bar = df.iloc[-2]

                    bar_time_str = target_bar.name.strftime("%H:%M")
                    record_key = f"{sym}_{bar_time_str}"

                    # 盤中常駐時，同一根 5K 只推播一次 (手動測試時則允許顯示)
                    if not is_manual_test and record_key in ALERTED_RECORDS:
                        continue

                    current_5k_vol = float(target_bar["Volume"])
                    vol_lots = int(current_5k_vol / 1000)
                    if vol_lots < MIN_5K_LOTS:
                        continue

                    avg_5k_vol = BASELINE_AVG_5K.get(sym, 0)
                    if avg_5k_vol <= 0:
                        continue

                    vol_ratio = current_5k_vol / avg_5k_vol
                    if vol_ratio < VOL_MULTIPLIER:
                        continue

                    open_p = float(target_bar["Open"])
                    close_p = float(target_bar["Close"])
                    low_p = float(target_bar["Low"])
                    high_p = float(target_bar["High"])
                    prev_close_p = float(prev_bar["Close"])
                    day_open_p = float(df["Open"].iloc[0])

                    # 【全方位急拉演算法】：支援三種主力點火型態
                    # 1. 實體紅K急拉：(本根收盤 - 本根開盤) / 本根開盤
                    body_pct = ((close_p - open_p) / open_p) * 100 if open_p > 0 else 0
                    # 2. 跨K棒跳空急拉：(本根收盤 - 前根收盤) / 前根收盤
                    jump_pct = ((close_p - prev_close_p) / prev_close_p) * 100 if prev_close_p > 0 else 0
                    # 3. 破底翻長下影急拉 (含收黑探底)：(本根收盤 - 本根最低) / 本根最低
                    rebound_pct = ((close_p - low_p) / low_p) * 100 if low_p > 0 else 0

                    # 排除「開高走低長上影線倒貨黑K」(收盤價必須位在當根 5K 高低振幅的上半部 >= 50%)
                    bar_range = high_p - low_p
                    close_pos = (close_p - low_p) / bar_range if bar_range > 0 else 1.0
                    if close_pos < 0.5:
                        continue

                    instant_pct = max(body_pct, jump_pct, rebound_pct)

                    if instant_pct >= PCT_THRESHOLD:
                        ALERTED_RECORDS.add(record_key)
                        clean_code = sym.replace(".TW", "").replace(".TWO", "")
                        stock_name = STOCK_DICT.get(sym, "")
                        avg_lots = max(1, int(avg_5k_vol / 1000))
                        futures_tag = " 【⚡有股期】" if clean_code in FUTURES_SET else ""

                        if close_p < open_p:
                            pattern_tag = "🔨收黑長下影探底"
                        elif close_p < day_open_p:
                            pattern_tag = "🟢低檔深V急拉"
                        else:
                            pattern_tag = "🔴強勢點火突破"

                        alerts.append({
                            "has_futures": clean_code in FUTURES_SET,
                            "vol_ratio": vol_ratio,
                            "msg": (
                                f"🔥 *{clean_code} {stock_name}*{futures_tag}\n"
                                f"🏷️ 型態：`{pattern_tag}`\n"
                                f"⏰ K棒：`{bar_time_str}` | 💰 現價：`{close_p:.2f}`\n"
                                f"🚀 5K拉升：`+{instant_pct:.2f}%` (5K低點 `{low_p:.2f}`)\n"
                                f"💥 5K爆量：`{vol_ratio:.1f} 倍` (`{vol_lots:,}`張 / 均量`{avg_lots:,}`張)"
                            )
                        })
                except Exception:
                    continue
        except Exception as e:
            print(f"⚠️ 批次掃描異常: {e}")

    elapsed = time.time() - start_t
    print(f"🏁 掃描完成！耗時 {elapsed:.1f} 秒，抓到 {len(alerts)} 檔強勢股。")

    if alerts:
        # 優先將【⚡有股期】且爆量倍數最大的排在最上方
        alerts.sort(key=lambda x: (x["has_futures"], x["vol_ratio"]), reverse=True)
        for idx in range(0, len(alerts), 12):
            chunk = alerts[idx:idx + 12]
            header = (
                f"⚡ *【全市場 5K 爆量急拉雷達】*\n"
                f"🕒 推播：{now_tw.strftime('%H:%M:%S')} (掃描 `{elapsed:.1f}秒`)\n"
                f"📌 條件：5K量>{VOL_MULTIPLIER}倍 & 拉升>{PCT_THRESHOLD}%\n"
                f"──────────────────\n\n"
            )
            body = "\n\n".join([item["msg"] for item in chunk])
            send_telegram(header + body)
    elif is_manual_test or SEND_HEARTBEAT:
        status_msg = (
            f"📡 *【5K 雷達盤中心跳回報】*\n"
            f"🕒 時間：`{now_tw.strftime('%H:%M:%S')}` (極速耗時 `{elapsed:.1f}秒`)\n"
            f"🎯 高速池：`{len(ACTIVE_UNIVERSE):,}` 檔活躍股 (含 `{len(FUTURES_SET)}` 檔股期)\n"
            f"📊 本根 5K 暫無同時滿足「爆量{VOL_MULTIPLIER}倍 + 拉升{PCT_THRESHOLD}%」之標的。"
        )
        send_telegram(status_msg)

def main():
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)
    is_weekday = now_tw.weekday() < 5
    current_hm = now_tw.hour * 100 + now_tw.minute

    # 非盤中時段 (週末、早於 08:20、或晚於 13:35)：執行單次測試並回報結果
    if not is_weekday or current_hm > 1335 or current_hm < 820:
        print("🔧 目前為非盤中時段，執行【單次極速掃描測試】...")
        run_single_scan(is_manual_test=True)
        return

    print("🔥 進入【盤中常駐高頻守護模式】...")
    init_market_and_baseline()
    send_telegram(
        f"🚀 *【盤中 5K 常駐雷達已啟動上線】*\n"
        f"🕒 啟動時間：`{datetime.now(tz_tw).strftime('%H:%M:%S')}`\n"
        f"✅ 已完成全市場預熱：鎖定 `{len(ACTIVE_UNIVERSE):,}` 檔活躍股與 `{len(FUTURES_SET)}` 檔個股期貨！\n"
        f"⚡ 將每 5 分鐘無延遲極速掃描至 13:35 收盤！"
    )

    last_scanned_slot = None
    now_tw = datetime.now(tz_tw)
    current_hm = now_tw.hour * 100 + now_tw.minute

    # 若在盤中 (09:05 後) 才剛啟動，立刻先掃描當下這一輪，不用乾等下一個 5 分鐘！
    if current_hm >= 905:
        slot_min = (now_tw.minute // 5) * 5
        last_scanned_slot = f"{now_tw.hour:02d}:{slot_min:02d}"
        run_single_scan(is_manual_test=False)

    # 【智慧時間槽常駐迴圈】：保證每 5 分鐘區間 (09:05, 09:10, 09:15...) 100% 觸發，絕不因延遲而漏棒！
    while True:
        now_tw = datetime.now(tz_tw)
        current_hm = now_tw.hour * 100 + now_tw.minute

        if current_hm > 1335:
            send_telegram("🔔 *【台股已收盤】* 今日 5K 常駐雷達監控任務圓滿結束！")
            print("🔔 台股已收盤，結束常駐程式。")
            break

        if current_hm >= 905:
            slot_min = (now_tw.minute // 5) * 5
            current_slot = f"{now_tw.hour:02d}:{slot_min:02d}"

            # 只要進入新的 5 分鐘區間，且距離 5 分鐘整點已過 8 秒 (讓 Yahoo K棒資料寫入完畢)，立刻掃描！
            elapsed_in_slot = (now_tw.minute % 5) * 60 + now_tw.second
            if current_slot != last_scanned_slot and elapsed_in_slot >= 8:
                last_scanned_slot = current_slot
                run_single_scan(is_manual_test=False)

        time.sleep(1)

if __name__ == "__main__":
    main()

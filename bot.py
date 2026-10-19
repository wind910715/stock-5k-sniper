import os
import time
from datetime import datetime
import pytz
import requests
import urllib3
import pandas as pd
import yfinance as yf

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ==================== 實戰策略參數設定 ====================
VOL_MULTIPLIER = 3.0       # 爆量倍數：單根 5K 量 >= 過去 5 日 5K 均量的 3.0 倍
PCT_THRESHOLD = 2.0        # 急拉門檻：單根 5K 瞬間拉升幅度 >= 2.0%
MIN_5K_LOTS = 100          # 單根 5K 最低張數：至少 >= 100 張 (過濾冷門股)
BATCH_SIZE = 180           # 每批併發下載檔數
SEND_HEARTBEAT = True      # 設為 True：每 5 分鐘掃描完若 0 檔也會發 TG 報平安，確認盤中有在跑！
# ==========================================================

STOCK_DICT = {}            # 股票代號 -> 中文名稱
FUTURES_SET = set()        # 擁有「個股期貨」的股票代號集合
BASELINE_AVG_5K = {}       # 快取：過去 5 日 5K 均量 (開盤算一次，整天 0 秒讀取)
ACTIVE_UNIVERSE = []       # 盤中活躍股池 (排除長年日均量不到 300 張的殭屍股)
ALERTED_RECORDS = set()    # 防止同一根 5K 重複推播

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ 未設定 TELEGRAM_TOKEN 或 TELEGRAM_CHAT_ID")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        res = requests.post(url, json=payload, timeout=10)
        if res.status_code != 200:
            print(f"TG 發送失敗: {res.text}")
    except Exception as e:
        print(f"TG 發送異常: {e}")

def init_market_and_baseline():
    """
    【開盤一次性預熱】
    1. 取得全市場上市櫃 + ETF 名單與個股期貨清單
    2. 計算過去 5 日 5K 均量，並直接剔除「過去 5 日平均日成交量 < 300 張」的冷門殭屍股！
       這樣盤中每 5 分鐘只需要掃描約 700~900 檔真正有流動性的股票，極速又不會被證交所舊資料誤導！
    """
    global STOCK_DICT, FUTURES_SET, BASELINE_AVG_5K, ACTIVE_UNIVERSE
    if ACTIVE_UNIVERSE:
        return

    print("📥 [1/3] 正在載入全市場股票名稱與期交所【個股期貨】清單...")
    headers = {'User-Agent': 'Mozilla/5.0'}

    # 載入個股期貨清單
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

    # 載入上市(TWSE)與上櫃(TPEx)完整代碼與中文名稱
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

    # 若官方 OpenAPI 維護中，使用 ISIN 網頁備援
    if len(STOCK_DICT) < 1000:
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
    print(f"📥 [2/3] 成功取得全市場 {len(all_symbols)} 檔標的，開始建立【5日均量快取】並剔除無量殭屍股...")

    for i in range(0, len(all_symbols), 250):
        batch = all_symbols[i:i + 250]
        try:
            data_d = yf.download(tickers=batch, period="6d", interval="1d", group_by="ticker", threads=True, progress=False)
            if data_d.empty:
                continue
            for sym in batch:
                try:
                    df_d = data_d if len(batch) == 1 else data_d[sym]
                    df_d = df_d.dropna(subset=["Volume"])
                    hist_df = df_d.iloc[:-1].tail(5) if len(df_d) > 1 else df_d
                    hist_df = hist_df[hist_df["Volume"] > 0]
                    if len(hist_df) > 0:
                        avg_daily_vol = float(hist_df["Volume"].mean())
                        # 保留過去 5 日平均日成交量 >= 250 張 (或有發行個股期貨) 的標的進入盤中高速池
                        clean_c = sym.replace(".TW", "").replace(".TWO", "")
                        if avg_daily_vol >= 250000 or clean_c in FUTURES_SET:
                            BASELINE_AVG_5K[sym] = avg_daily_vol / 54.0
                            ACTIVE_UNIVERSE.append(sym)
                except Exception:
                    continue
        except Exception as e:
            print(f"預熱批次異常: {e}")

    print(f"✅ [3/3] 預熱完成！鎖定 {len(ACTIVE_UNIVERSE)} 檔高流動性活躍股與股期標的，之後每 5 分鐘僅需極速下載 1d 資料！")

def run_single_scan(is_manual_test=False):
    start_t = time.time()
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)

    init_market_and_baseline()

    if not ACTIVE_UNIVERSE:
        print("❌ 活躍股池為空，略過本次掃描。")
        return

    print(f"[{now_tw.strftime('%H:%M:%S')}] ⚡ 開始掃描 {len(ACTIVE_UNIVERSE)} 檔盤中即時 5K...")
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

                    # 未收線保護：若最後一根 5K 產生還不到 4 分 10 秒 (250秒)，取前一根完整收線的 5K
                    last_bar_time = df.index[-1]
                    if (now_tw - last_bar_time).total_seconds() < 250 and len(df) >= 3:
                        target_bar = df.iloc[-2]
                        prev_bar = df.iloc[-3]
                    else:
                        target_bar = df.iloc[-1]
                        prev_bar = df.iloc[-2]

                    bar_time_str = target_bar.name.strftime("%H:%M")
                    record_key = f"{sym}_{bar_time_str}"
                    if record_key in ALERTED_RECORDS:
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

                    # 三合一急拉判斷：實體紅K急拉、跨K跳空急拉、破底翻長下影線急拉
                    body_pct = ((close_p - open_p) / open_p) * 100 if open_p > 0 else 0
                    jump_pct = ((close_p - prev_close_p) / prev_close_p) * 100 if prev_close_p > 0 else 0
                    rebound_pct = ((close_p - low_p) / low_p) * 100 if low_p > 0 else 0

                    # 排除開高走低倒貨黑K (收盤需在當根 5K 振幅上半部)
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
            print(f"批次掃描異常: {e}")

    elapsed = time.time() - start_t
    print(f"🏁 掃描完成！耗時 {elapsed:.1f} 秒，抓到 {len(alerts)} 檔強勢股。")

    if alerts:
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
            f"🕒 時間：`{now_tw.strftime('%H:%M:%S')}` (掃描耗時 `{elapsed:.1f}秒`)\n"
            f"🎯 監控池：`{len(ACTIVE_UNIVERSE):,}` 檔活躍股 (含 `{len(FUTURES_SET)}` 檔股期)\n"
            f"📊 本根 5K 暫無同時滿足「爆量{VOL_MULTIPLIER}倍 + 急拉{PCT_THRESHOLD}%」之標的。"
        )
        send_telegram(status_msg)

def main():
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)
    is_weekday = now_tw.weekday() < 5
    current_hm = now_tw.hour * 100 + now_tw.minute

    if not is_weekday or current_hm > 1335 or current_hm < 845:
        print("🔧 目前為非盤中時段，執行【單次極速掃描測試】...")
        run_single_scan(is_manual_test=True)
        return

    print("🔥 進入【盤中常駐高頻模式】...")
    # 啟動時先發送上線通知，並預熱建立全市場 5 日均量快取！
    init_market_and_baseline()
    send_telegram(
        f"🚀 *【盤中 5K 常駐雷達已啟動上線】*\n"
        f"🕒 啟動時間：`{datetime.now(tz_tw).strftime('%H:%M:%S')}`\n"
        f"✅ 已預熱完成 `{len(ACTIVE_UNIVERSE):,}` 檔活躍股與 `{len(FUTURES_SET)}` 檔個股期貨均量快取！\n"
        f"⚡ 接下來每 5 分鐘將極速掃描並推播至 13:35 收盤！"
    )

    # 若是在盤中 (例如 09:18) 才剛啟動，立刻先掃描一次當下的 K 棒，不用乾等下一個 5 分鐘！
    if current_hm >= 905:
        run_single_scan(is_manual_test=False)

    while True:
        now_tw = datetime.now(tz_tw)
        current_hm = now_tw.hour * 100 + now_tw.minute
        if current_hm > 1335:
            send_telegram("🔔 *【台股已收盤】* 今日 5K 常駐雷達任務圓滿結束！")
            break

        # 每 5 分鐘整點過後的第 10 秒 (如 09:20:10, 09:25:10) 準時掃描剛收完線的 5K
        if now_tw.minute % 5 == 0 and now_tw.second >= 10:
            if current_hm >= 905:
                run_single_scan(is_manual_test=False)
            time.sleep(60)
        else:
            time.sleep(1)

if __name__ == "__main__":
    main()

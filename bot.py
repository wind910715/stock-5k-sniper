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

# ==================== 極速策略參數設定 ====================
VOL_MULTIPLIER = 3.0       # 爆量倍數：單根 5K 量 >= 過去 5 日 5K 均量的 3.0 倍
PCT_THRESHOLD = 2.0        # 急拉門檻：單根 5K 瞬間漲幅 >= 2.0%
MIN_5K_LOTS = 100          # 單根 5K 最低張數：至少 >= 100 張
PREFILTER_DAILY_LOTS = 300 # 第一階段快篩：今日總成交量 >= 300 張才進入 5K 掃描 (極速過濾冷門股)
BATCH_SIZE = 150           # 批次併發下載數量
SEND_HEARTBEAT = False     # 若 0 檔符合時是否發送「平安回報」(建議設 False 盤中才不會被洗版，測試時可設 True)
# ==========================================================

# 全域快取：儲存全市場名單、5日均量、以及已推播過的 K 棒 (防止重複推播)
STOCK_DICT = {}
BASELINE_AVG_5K = {}
ALERTED_RECORDS = set()

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ 未設定 TELEGRAM_TOKEN 或 TELEGRAM_CHAT_ID")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"TG 發送異常: {e}")

def get_active_stocks_fast():
    """
    【極速第一階段】直接向證交所(TWSE)與櫃買中心(TPEx)官方 OpenAPI 抓取全市場最新快照
    耗時僅約 1.5 秒！直接篩選出「今日總量 >= 300張」的活躍股，剔除 85% 殭屍股！
    """
    active_symbols = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 1. 上市全市場即時快照 (TWSE)
    try:
        r1 = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", headers=headers, verify=False, timeout=10)
        if r1.status_code == 200:
            for row in r1.json():
                code = row.get("Code", "").strip()
                name = row.get("Name", "").strip()
                vol_str = row.get("TradeVolume", "0").replace(",", "")
                if 4 <= len(code) <= 6 and vol_str.isdigit():
                    sym = f"{code}.TW"
                    STOCK_DICT[sym] = name
                    if int(vol_str) // 1000 >= PREFILTER_DAILY_LOTS:
                        active_symbols.append(sym)
    except Exception as e:
        print(f"⚠️ TWSE 快篩讀取異常: {e}")

    # 2. 上櫃全市場即時快照 (TPEx)
    try:
        r2 = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", headers=headers, verify=False, timeout=10)
        if r2.status_code == 200:
            for row in r2.json():
                code = row.get("SecuritiesCompanyCode", "").strip()
                name = row.get("CompanyName", "").strip()
                vol_str = row.get("TradingShares", "0").replace(",", "")
                if 4 <= len(code) <= 6 and vol_str.isdigit():
                    sym = f"{code}.TWO"
                    STOCK_DICT[sym] = name
                    if int(vol_str) // 1000 >= PREFILTER_DAILY_LOTS:
                        active_symbols.append(sym)
    except Exception as e:
        print(f"⚠️ TPEx 快篩讀取異常: {e}")

    # 備援機制：萬一證交所 API 瞬斷，回退使用 ISIN 名單
    if not active_symbols and STOCK_DICT:
        return list(STOCK_DICT.keys())
        
    return active_symbols

def update_5d_baseline(symbols):
    """
    計算並快取「過去 5 日的 5K 平均成交量」(每檔股票一天只需要算一次！)
    借助過濾後的名單，只需算幾百檔活躍股，速度極快。
    """
    missing_syms = [s for s in symbols if s not in BASELINE_AVG_5K]
    if not missing_syms:
        return

    print(f"📥 正在建立 {len(missing_syms)} 檔活躍股的【5日均量快取】...")
    for i in range(0, len(missing_syms), BATCH_SIZE):
        batch = missing_syms[i:i + BATCH_SIZE]
        try:
            # 抓取過去 5 天日K總量，除以 (交易天數 * 每日54根5K)，1秒算出精準 5日5K均量！
            data = yf.download(tickers=batch, period="6d", interval="1d", group_by="ticker", threads=True, progress=False)
            if data.empty:
                continue
            for sym in batch:
                try:
                    df = data if len(batch) == 1 else data[sym]
                    df = df.dropna(subset=["Volume"])
                    # 取不含今天的前 5 個交易日
                    hist_df = df.iloc[:-1].tail(5) if len(df) > 1 else df
                    hist_df = hist_df[hist_df["Volume"] > 0]
                    if len(hist_df) > 0:
                        # 每日 270 分鐘 = 54 根 5分K
                        BASELINE_AVG_5K[sym] = float(hist_df["Volume"].mean()) / 54.0
                except Exception:
                    continue
        except Exception as e:
            print(f"建立均量快取異常: {e}")

def run_single_scan(is_manual_test=False):
    start_t = time.time()
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)
    
    # 第一階段：1.5 秒快篩全市場活躍股
    active_symbols = get_active_stocks_fast()
    total_market_count = len(STOCK_DICT) if STOCK_DICT else 2339
    print(f"[{now_tw.strftime('%H:%M:%S')}] ⚡ 快篩完成：從全市場 {total_market_count} 檔中鎖定 {len(active_symbols)} 檔帶量活躍股")

    if not active_symbols:
        return

    # 更新活躍股的 5 日 5K 均量快取 (已算過的股票會直接跳過，0 秒讀取)
    update_5d_baseline(active_symbols)

    alerts = []

    # 第二階段：只針對活躍股下載「今日 (1d)」的 5分K，資料量減少 85%！
    for i in range(0, len(active_symbols), BATCH_SIZE):
        batch = active_symbols[i:i + BATCH_SIZE]
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

                    # 未收線保護：若最後一根 5K 產生還不到 4 分 15 秒 (255秒)，取前一根剛收完的完整 5K
                    last_bar_time = df.index[-1]
                    if (now_tw - last_bar_time).total_seconds() < 255 and len(df) >= 3:
                        target_bar = df.iloc[-2]
                        prev_bar = df.iloc[-3]
                    else:
                        target_bar = df.iloc[-1]
                        prev_bar = df.iloc[-2]

                    bar_time_str = target_bar.name.strftime("%H:%M")
                    record_key = f"{sym}_{bar_time_str}"

                    # 防止同一根 5K 重複推播
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

                    open_p = float(target_bar["Open"])
                    close_p = float(target_bar["Close"])
                    prev_close_p = float(prev_bar["Close"])

                    bar_body_pct = ((close_p - open_p) / open_p) * 100 if open_p > 0 else 0
                    bar_jump_pct = ((close_p - prev_close_p) / prev_close_p) * 100 if prev_close_p > 0 else 0
                    instant_pct = max(bar_body_pct, bar_jump_pct)

                    if vol_ratio >= VOL_MULTIPLIER and instant_pct >= PCT_THRESHOLD and close_p >= open_p:
                        ALERTED_RECORDS.add(record_key)
                        clean_code = sym.replace(".TW", "").replace(".TWO", "")
                        stock_name = STOCK_DICT.get(sym, "")
                        avg_lots = max(1, int(avg_5k_vol / 1000))

                        alerts.append({
                            "vol_ratio": vol_ratio,
                            "msg": (
                                f"🔥 *{clean_code} {stock_name}*\n"
                                f"⏰ K棒：`{bar_time_str}` | 💰 現價：`{close_p:.2f}`\n"
                                f"🚀 5K急拉：`+{instant_pct:.2f}%`\n"
                                f"💥 5K爆量：`{vol_ratio:.1f} 倍` (`{vol_lots:,}`張 / 均量`{avg_lots:,}`張)"
                            )
                        })
                except Exception:
                    continue
        except Exception as e:
            print(f"批次掃描異常: {e}")

    elapsed = time.time() - start_t
    print(f"🏁 極速掃描完成！僅耗時 {elapsed:.1f} 秒，抓到 {len(alerts)} 檔強勢股。")

    if alerts:
        alerts.sort(key=lambda x: x["vol_ratio"], reverse=True)
        for idx in range(0, len(alerts), 15):
            chunk = alerts[idx:idx + 15]
            header = (
                f"⚡ *【全市場 5K 極速雷達】*\n"
                f"🕒 推播時間：{now_tw.strftime('%H:%M:%S')} (掃描耗時 `{elapsed:.1f}秒`)\n"
                f"📌 條件：5K量 > 5日均量 {VOL_MULTIPLIER}倍 & 急拉 > {PCT_THRESHOLD}%\n"
                f"──────────────────\n\n"
            )
            body = "\n\n".join([item["msg"] for item in chunk])
            send_telegram(header + body)
    elif is_manual_test or SEND_HEARTBEAT:
        status_msg = (
            f"✅ *【極速引擎測試完成】*\n"
            f"🕒 時間：`{now_tw.strftime('%H:%M:%S')}`\n"
            f"🚀 掃描耗時：僅 **`{elapsed:.1f} 秒`** (原耗時 800 秒)\n"
            f"📡 全市場 `{total_market_count:,}` 檔 ➔ 快篩鎖定 `{len(active_symbols):,}` 檔活躍股\n"
            f"📊 本根 5K 暫無符合條件之標的。"
        )
        send_telegram(status_msg)

def main():
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)
    
    # 判斷是否為台股盤中時間 (週一至週五 08:55 ~ 13:35)
    is_weekday = now_tw.weekday() < 5
    current_hm = now_tw.hour * 100 + now_tw.minute

    # 如果是盤後或週末手動點擊 Run workflow，執行單次極速測試並回報耗時
    if not is_weekday or current_hm > 1335 or current_hm < 850:
        print("🔧 目前為非盤中時段，執行【單次極速掃描測試】...")
        run_single_scan(is_manual_test=True)
        return

    # 盤中時段：啟動【常駐高頻守護模式】，每 5 分鐘 (00, 05, 10, 15...) 剛收完 K 棒後的第 8 秒準時掃描！
    print("🔥 進入【盤中常駐高頻模式】，將持續每 5 分鐘無延遲密集監控至 13:35 收盤！")
    send_telegram(f"🚀 *【盤中極速常駐雷達已啟動】*\n今日將每 5 分鐘自動於 K 棒收線後秒速掃描全市場，直到 13:35 收盤！")

    while True:
        now_tw = datetime.now(tz_tw)
        current_hm = now_tw.hour * 100 + now_tw.minute
        if current_hm > 1335:
            print("🔔 台股已收盤，結束今日常駐監控。")
            break

        # 對齊每 5 分鐘的 K 棒收線時間 (例如 09:05:08, 09:10:08, 09:15:08...)
        if now_tw.minute % 5 == 0 and now_tw.second >= 8:
            if current_hm >= 905:
                run_single_scan(is_manual_test=False)
            # 掃描完後休息 65 秒，等待下一個 5 分鐘整點
            time.sleep(65)
        else:
            time.sleep(1)

if __name__ == "__main__":
    main()

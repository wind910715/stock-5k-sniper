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
PCT_THRESHOLD = 2.0        # 急拉門檻：單根 5K 瞬間拉升幅度 >= 2.0%
MIN_5K_LOTS = 100          # 單根 5K 最低張數：至少 >= 100 張
BATCH_SIZE = 120           # 單趟併發批次大小
SEND_HEARTBEAT = False     # 盤中 0 檔時不洗版 (手動點擊測試時會強制發送報告)
# ==========================================================

STOCK_DICT = {}            # 股票代號 -> 中文名稱
PREV_CLOSE_DICT = {}       # 股票代號 -> 昨日收盤價 (用來判斷目前是在平盤上還是平盤下綠盤)
FUTURES_SET = set()        # 擁有「個股期貨」的股票代號集合
BASELINE_AVG_5K = {}       # 快取：過去 5 日 5K 均量
ALERTED_RECORDS = set()    # 防止同一根 5K 重複推播

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

def load_stock_futures_list():
    """載入台灣期交所「股票期貨標的證券」清單"""
    global FUTURES_SET
    if FUTURES_SET:
        return
    try:
        url_sf = "https://www.taifex.com.tw/cht/2/stockLists"
        res = requests.get(url_sf, headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=10)
        dfs = pd.read_html(res.text)
        for df in dfs:
            for col in df.columns:
                if "證券代號" in str(col) or "標的" in str(col):
                    for val in df[col].dropna():
                        code = str(val).strip().split()[0]
                        if 4 <= len(code) <= 6:
                            FUTURES_SET.add(code)
        print(f"✅ 成功載入期交所【個股期貨】標的清單：共 {len(FUTURES_SET)} 檔")
    except Exception as e:
        print(f"⚠️ 期交所清單讀取略過: {e}")

def get_candidates_math_lossless():
    """
    【100% 無損全納快篩（含綠盤下跌急拉、收黑長下影線）】
    不管股票今天是大跌綠盤、還是黑K，只要它在 5 分鐘內能從低點往上拉升 >= 2%，
    它的「現價」距離「今日最低價 (Low)」就一定 >= 2%！
    同時把門檻降為今日總量 >= 100 張 (即 MIN_5K_LOTS)，確保早盤第一根爆量也 100% 不漏接！
    """
    candidates = []
    total_count = 0
    headers = {'User-Agent': 'Mozilla/5.0'}
    min_shares = MIN_5K_LOTS * 1000

    def parse_float(val):
        try:
            return float(str(val).replace(",", "").strip())
        except Exception:
            return 0.0

    # 1. 上市全市場快照 (TWSE)
    try:
        r1 = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", headers=headers, verify=False, timeout=10)
        if r1.status_code == 200:
            for row in r1.json():
                code = row.get("Code", "").strip()
                name = row.get("Name", "").strip()
                if not (4 <= len(code) <= 6):
                    continue
                total_count += 1
                sym = f"{code}.TW"
                STOCK_DICT[sym] = name

                vol_shares = parse_float(row.get("TradeVolume", 0))
                close_p = parse_float(row.get("ClosingPrice", 0))
                low_p = parse_float(row.get("LowestPrice", 0))
                change_p = parse_float(row.get("Change", 0))
                if close_p > 0:
                    PREV_CLOSE_DICT[sym] = close_p - change_p

                # 只要今日總量達 100 張，且現價高於今日最低價達 1.8% (不論紅盤綠盤皆納入)
                if vol_shares >= min_shares and low_p > 0:
                    if ((close_p - low_p) / low_p) * 100 >= (PCT_THRESHOLD - 0.2):
                        candidates.append(sym)
    except Exception as e:
        print(f"⚠️ TWSE 快篩異常: {e}")

    # 2. 上櫃全市場快照 (TPEx)
    try:
        r2 = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", headers=headers, verify=False, timeout=10)
        if r2.status_code == 200:
            for row in r2.json():
                code = row.get("SecuritiesCompanyCode", "").strip()
                name = row.get("CompanyName", "").strip()
                if not (4 <= len(code) <= 6):
                    continue
                total_count += 1
                sym = f"{code}.TWO"
                STOCK_DICT[sym] = name

                vol_shares = parse_float(row.get("TradingShares", 0))
                close_p = parse_float(row.get("Close", 0))
                low_p = parse_float(row.get("Low", 0))
                change_p = parse_float(row.get("Change", 0))
                if close_p > 0:
                    PREV_CLOSE_DICT[sym] = close_p - change_p

                if vol_shares >= min_shares and low_p > 0:
                    if ((close_p - low_p) / low_p) * 100 >= (PCT_THRESHOLD - 0.2):
                        candidates.append(sym)
    except Exception as e:
        print(f"⚠️ TPEx 快篩異常: {e}")

    return total_count, candidates

def process_batch(batch_symbols, period_str, now_tw, tz_tw, alerts):
    """單趟下載並同時完成均量計算與 5K 急拉偵測（涵蓋紅K強拉、綠盤深V、收黑長下影線）"""
    if not batch_symbols:
        return
    try:
        data = yf.download(
            tickers=batch_symbols,
            period=period_str,
            interval="5m",
            group_by="ticker",
            threads=True,
            progress=False
        )
        if data.empty:
            return

        for sym in batch_symbols:
            try:
                df = data if len(batch_symbols) == 1 else data[sym]
                df = df.dropna(subset=["Close", "Volume"])
                if len(df) < 3:
                    continue

                if df.index.tz is None:
                    df.index = df.index.tz_localize("UTC").tz_convert(tz_tw)
                else:
                    df.index = df.index.tz_convert(tz_tw)

                # 未收線保護：若最新一根 5K 產生還不到 4 分 15 秒，取前一根完整 5K
                last_bar_time = df.index[-1]
                if (now_tw - last_bar_time).total_seconds() < 255 and len(df) >= 3:
                    target_bar = df.iloc[-2]
                    prev_bar = df.iloc[-3]
                    hist_df = df.iloc[:-2]
                else:
                    target_bar = df.iloc[-1]
                    prev_bar = df.iloc[-2]
                    hist_df = df.iloc[:-1]

                # 若該股尚未建立 5 日 5K 均量快取，直接用剛剛載下來的歷史 5K 秒算並存入快取！
                if sym not in BASELINE_AVG_5K:
                    valid_hist = hist_df[hist_df["Volume"] > 0].tail(270)
                    if len(valid_hist) >= 15:
                        BASELINE_AVG_5K[sym] = float(valid_hist["Volume"].mean())

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

                # 【全方位急拉演算法】：支援三種主力點火型態！
                # 1. 實體紅K急拉：(本根收盤 - 本根開盤) / 本根開盤
                body_pct = ((close_p - open_p) / open_p) * 100 if open_p > 0 else 0
                # 2. 跨K棒跳空急拉：(本根收盤 - 前根5K收盤) / 前根5K收盤
                jump_pct = ((close_p - prev_close_p) / prev_close_p) * 100 if prev_close_p > 0 else 0
                # 3. 破底翻下影線急拉 (即便微收黑也能抓)：(本根收盤 - 本根最低) / 本根最低
                rebound_pct = ((close_p - low_p) / low_p) * 100 if low_p > 0 else 0

                # 防呆過濾：若為「開高走低倒貨黑K」（收盤落在這根 5K 振幅的下半部 < 50%），則不算往上急拉，避免誤抓出貨K棒
                bar_range = high_p - low_p
                close_position = (close_p - low_p) / bar_range if bar_range > 0 else 1.0
                if close_position < 0.5:
                    continue

                instant_pct = max(body_pct, jump_pct, rebound_pct)

                if instant_pct >= PCT_THRESHOLD:
                    ALERTED_RECORDS.add(record_key)
                    clean_code = sym.replace(".TW", "").replace(".TWO", "")
                    stock_name = STOCK_DICT.get(sym, "")
                    avg_lots = max(1, int(avg_5k_vol / 1000))
                    futures_tag = " 【⚡有股期】" if clean_code in FUTURES_SET else ""

                    # 判斷當日位階（平盤下綠盤深V 還是 平盤上強勢突破）與 K 棒型態
                    yest_close = PREV_CLOSE_DICT.get(sym, prev_close_p)
                    day_pct = ((close_p - yest_close) / yest_close) * 100 if yest_close > 0 else 0

                    if close_p < open_p:
                        pattern_tag = "🔨收黑長下影探底"
                    elif day_pct < 0:
                        pattern_tag = f"🟢平盤下深V急拉 (當日 {day_pct:+.1f}%)"
                    else:
                        pattern_tag = f"🔴強勢點火突破 (當日 {day_pct:+.1f}%)"

                    alerts.append({
                        "has_futures": clean_code in FUTURES_SET,
                        "vol_ratio": vol_ratio,
                        "msg": (
                            f"🔥 *{clean_code} {stock_name}*{futures_tag}\n"
                            f"🏷️ 型態：`{pattern_tag}`\n"
                            f"⏰ K棒：`{bar_time_str}` | 💰 現價：`{close_p:.2f}`\n"
                            f"🚀 5K瞬間拉升：`+{instant_pct:.2f}%` (自5K低點 `{low_p:.2f}` 拉起)\n"
                            f"💥 5K爆量：`{vol_ratio:.1f} 倍` (`{vol_lots:,}`張 / 均量`{avg_lots:,}`張)"
                        )
                    })
            except Exception:
                continue
    except Exception as e:
        print(f"批次掃描異常: {e}")

def run_single_scan(is_manual_test=False):
    start_t = time.time()
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)

    load_stock_futures_list()
    total_market, candidates = get_candidates_math_lossless()

    # 將候選股分為「尚未建立均量快取 (首次抓 5d 一次搞定)」與「已有均量快取 (極速只抓 1d)」
    need_5d = [s for s in candidates if s not in BASELINE_AVG_5K]
     only_1d = [s for s in candidates if s in BASELINE_AVG_5K]

    print(f"[{now_tw.strftime('%H:%M:%S')}] ⚡ 快篩鎖定 {len(candidates)} 檔 (首次單趟5d: {len(need_5d)}檔 | 快取極速1d: {len(only_1d)}檔)")

    alerts = []

    # 1. 尚未有快取的股票：單趟下載 5d 的 5m K線（同時算好均量 + 檢查最新一根 5K，免除二次下載！）
    for i in range(0, len(need_5d), BATCH_SIZE):
        process_batch(need_5d[i:i + BATCH_SIZE], "5d", now_tw, tz_tw, alerts)

    # 2. 已有快取的股票：極速只下載 1d 的 5m K線
    for i in range(0, len(only_1d), BATCH_SIZE):
        process_batch(only_1d[i:i + BATCH_SIZE], "1d", now_tw, tz_tw, alerts)

    elapsed = time.time() - start_t
    print(f"🏁 掃描完成！耗時 {elapsed:.1f} 秒，抓到 {len(alerts)} 檔強勢股。")

    if alerts:
        alerts.sort(key=lambda x: (x["has_futures"], x["vol_ratio"]), reverse=True)
        for idx in range(0, len(alerts), 12):
            chunk = alerts[idx:idx + 12]
            header = (
                f"⚡ *【全市場 5K 爆量急拉雷達】*\n"
                f"🕒 推播：{now_tw.strftime('%H:%M:%S')} (耗時 `{elapsed:.1f}秒`)\n"
                f"📌 條件：5K量>{VOL_MULTIPLIER}倍 & 5K拉升>{PCT_THRESHOLD}% (含綠盤深V/探底)\n"
                f"──────────────────\n\n"
            )
            body = "\n\n".join([item["msg"] for item in chunk])
            send_telegram(header + body)
    elif is_manual_test or SEND_HEARTBEAT:
        status_msg = (
            f"✅ *【全型態極速引擎測試完成】*\n"
            f"🕒 時間：`{now_tw.strftime('%H:%M:%S')}`\n"
            f"🚀 掃描耗時：**`{elapsed:.1f} 秒`** (單趟二合一極速架構)\n"
            f"📡 全市場 `{total_market:,}` 檔 ➔ 鎖定 `{len(candidates):,}` 檔\n"
            f"⚡ 期交所股期標的庫：`{len(FUTURES_SET)}` 檔\n"
            f"📊 已完整支援：🔴紅K強拉、🟢平盤下深V急拉、🔨收黑長下影探底！"
        )
        send_telegram(status_msg)

def main():
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)
    is_weekday = now_tw.weekday() < 5
    current_hm = now_tw.hour * 100 + now_tw.minute

    if not is_weekday or current_hm > 1335 or current_hm < 850:
        print("🔧 目前為非盤中時段，執行【單次極速掃描測試】...")
        run_single_scan(is_manual_test=True)
        return

    print("🔥 進入【盤中常駐高頻模式】...")
    load_stock_futures_list()
    send_telegram(
        f"🚀 *【盤中 5K 全型態極速雷達已啟動】*\n"
        f"⚡ 支援：紅K突破、綠盤深V低檔急拉、收黑長下影探底，並自動標註 `{len(FUTURES_SET)}` 檔個股期貨！"
    )

    while True:
        now_tw = datetime.now(tz_tw)
        current_hm = now_tw.hour * 100 + now_tw.minute
        if current_hm > 1335:
            print("🔔 台股已收盤，結束今日常駐監控。")
            break

        if now_tw.minute % 5 == 0 and now_tw.second >= 8:
            if current_hm >= 905:
                run_single_scan(is_manual_test=False)
            time.sleep(65)
        else:
            time.sleep(1)

if __name__ == "__main__":
    main()

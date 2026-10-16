import os
import time
from datetime import datetime
import pytz
import requests
import urllib3
import pandas as pd
import yfinance as yf

# 關閉爬蟲抓證交所時的 SSL 警告訊息
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 讀取 GitHub Secrets 金鑰
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ==================== 策略參數設定區 ====================
VOL_MULTIPLIER = 3.0   # 爆量倍數門檻：單根 5K 成交量 >= 過去 5 日 5K 均量的 3 倍
PCT_THRESHOLD = 2.0    # 瞬間漲幅門檻：單根 5K 瞬間急拉 >= 2.0%
MIN_5K_LOTS = 100      # 最低流動性過濾：該根 5K 成交量至少要 >= 100 張
BATCH_SIZE = 200       # 每批次同時下載的檔數
# ========================================================

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ 錯誤：未設定 TELEGRAM_TOKEN 或 TELEGRAM_CHAT_ID，請檢查 GitHub Secrets！")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        res = requests.post(url, json=payload, timeout=15)
        if res.status_code == 200:
            print("✅ Telegram 訊息發送成功！")
        else:
            print(f"❌ Telegram 發送失敗 (狀態碼 {res.status_code}): {res.text}")
    except Exception as e:
        print(f"❌ 連線 Telegram 發生異常: {e}")

def get_all_tw_stocks():
    stock_dict = {}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    modes = [('2', '.TW'), ('4', '.TWO')]
    
    for mode, suffix in modes:
        url = f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}"
        try:
            res = requests.get(url, headers=headers, verify=False, timeout=15)
            res.encoding = 'big5'
            dfs = pd.read_html(res.text)
            df = dfs[0]
            df.columns = df.iloc[0]
            df = df.iloc[1:]
            
            if 'CFICode' in df.columns and '有價證券代號及名稱' in df.columns:
                cond = (df['CFICode'] == 'ESVUFR') | (df['CFICode'].astype(str).str.startswith('CEO'))
                valid_df = df[cond]
                
                for item in valid_df['有價證券代號及名稱']:
                    parts = str(item).replace('\u3000', ' ').split()
                    if len(parts) >= 2:
                        code, name = parts[0].strip(), parts[1].strip()
                        if 4 <= len(code) <= 6:
                            stock_dict[f"{code}{suffix}"] = name
        except Exception as e:
            print(f"⚠️ 抓取市場清單 (Mode {mode}) 發生異常: {e}")

    print(f"✅ 成功載入全市場清單：共 {len(stock_dict)} 檔上市櫃個股與 ETF")
    return stock_dict

def scan_full_market():
    start_time = time.time()
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)
    print(f"[{now_tw.strftime('%Y-%m-%d %H:%M:%S')}] 🚀 啟動全市場 5K 爆量急拉雷達...")

    stock_dict = get_all_tw_stocks()
    if not stock_dict:
        print("❌ 無法取得股票清單，結束掃描。")
        return

    symbols = list(stock_dict.keys())
    alerts = []

    for i in range(0, len(symbols), BATCH_SIZE):
        batch_symbols = symbols[i:i + BATCH_SIZE]
        print(f"📡 正在掃描第 {i+1} ~ {min(i+BATCH_SIZE, len(symbols))} 檔...")
        
        try:
            data = yf.download(
                tickers=batch_symbols,
                period="6d",
                interval="5m",
                group_by="ticker",
                threads=True,
                progress=False
            )
            
            if data.empty:
                continue

            for sym in batch_symbols:
                try:
                    if len(batch_symbols) == 1:
                        df = data
                    else:
                        if sym not in data.columns.get_level_values(0):
                            continue
                        df = data[sym]

                    df = df.dropna(subset=["Close", "Volume"])
                    if len(df) < 30:
                        continue

                    if df.index.tz is None:
                        df.index = df.index.tz_localize("UTC").tz_convert(tz_tw)
                    else:
                        df.index = df.index.tz_convert(tz_tw)

                    last_bar_time = df.index[-1]
                    if (now_tw - last_bar_time).total_seconds() < 270 and len(df) > 2:
                        target_bar = df.iloc[-2]
                        prev_bar = df.iloc[-3]
                        history_df = df.iloc[:-2]
                    else:
                        target_bar = df.iloc[-1]
                        prev_bar = df.iloc[-2]
                        history_df = df.iloc[:-1]

                    current_5k_vol = float(target_bar["Volume"])
                    vol_lots = int(current_5k_vol / 1000)

                    if vol_lots < MIN_5K_LOTS:
                        continue

                    valid_history = history_df[history_df["Volume"] > 0].tail(270)
                    if len(valid_history) < 20:
                        continue

                    avg_5k_vol = float(valid_history["Volume"].mean())
                    vol_ratio = current_5k_vol / avg_5k_vol if avg_5k_vol > 0 else 0

                    open_p = float(target_bar["Open"])
                    close_p = float(target_bar["Close"])
                    prev_close_p = float(prev_bar["Close"])

                    bar_body_pct = ((close_p - open_p) / open_p) * 100 if open_p > 0 else 0
                    bar_jump_pct = ((close_p - prev_close_p) / prev_close_p) * 100 if prev_close_p > 0 else 0
                    instant_pct = max(bar_body_pct, bar_jump_pct)

                    if vol_ratio >= VOL_MULTIPLIER and instant_pct >= PCT_THRESHOLD and close_p >= open_p:
                        clean_code = sym.replace(".TW", "").replace(".TWO", "")
                        stock_name = stock_dict.get(sym, "")
                        avg_lots = max(1, int(avg_5k_vol / 1000))
                        bar_time_str = target_bar.name.strftime("%H:%M")

                        alert_item = {
                            "vol_ratio": vol_ratio,
                            "msg": (
                                f"🔥 *{clean_code} {stock_name}*\n"
                                f"⏰ K棒：`{bar_time_str}` | 💰 現價：`{close_p:.2f}`\n"
                                f"🚀 5K急拉：`+{instant_pct:.2f}%`\n"
                                f"💥 5K爆量：`{vol_ratio:.1f} 倍` (`{vol_lots:,}`張 / 均量`{avg_lots:,}`張)"
                            )
                        }
                        alerts.append(alert_item)

                except Exception:
                    continue

        except Exception as e:
            print(f"批次下載發生異常: {e}")

    elapsed = time.time() - start_time
    print(f"🏁 全市場掃描完成！共耗時 {elapsed:.1f} 秒，偵測到 {len(alerts)} 檔符合條件。")

    if alerts:
        alerts.sort(key=lambda x: x["vol_ratio"], reverse=True)
        chunk_size = 15
        for idx in range(0, len(alerts), chunk_size):
            chunk = alerts[idx:idx + chunk_size]
            header = (
                f"⚡ *【全市場 5K 爆量急拉雷達】*\n"
                f"🕒 掃描時間：{now_tw.strftime('%H:%M')} (耗時 {elapsed:.0f}秒)\n"
                f"📌 條件：5K量 > 5日均量 {VOL_MULTIPLIER}倍 & 急拉 > {PCT_THRESHOLD}%\n"
                f"──────────────────\n\n"
            )
            body = "\n\n".join([item["msg"] for item in chunk])
            send_telegram(header + body)
    else:
        # 保底回報：即使 0 檔也會發送 Telegram 確認連線成功
        status_msg = (
            f"✅ *【全市場雷達連線正常】*\n"
            f"🕒 掃描時間：`{now_tw.strftime('%H:%M:%S')}`\n"
            f"📡 已掃描全市場：`{len(stock_dict):,}` 檔上市櫃個股與 ETF (耗時 {elapsed:.0f}秒)\n"
            f"📊 本時段暫無同時滿足「5K量>{VOL_MULTIPLIER}倍 + 急拉>{PCT_THRESHOLD}%」之標的。"
        )
        send_telegram(status_msg)

if __name__ == "__main__":
    scan_full_market()

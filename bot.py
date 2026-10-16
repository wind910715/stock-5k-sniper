import os
from datetime import datetime
import pytz
import requests
import yfinance as yf
import pandas as pd

# 讀取 GitHub Secrets 金鑰
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 監控股票清單（上市加 .TW，上櫃加 .TWO）
# 建議放入您的核心觀察股或高波動中小型股
WATCHLIST = [
    "2330.TW", "2454.TW", "2383.TW", "3711.TW", 
    "2317.TW", "3037.TW", "3017.TW", "3661.TW"
]

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ 錯誤：未設定 TELEGRAM_TOKEN 或 TELEGRAM_CHAT_ID")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        res = requests.post(url, json=payload, timeout=10)
        if res.status_code != 200:
            print(f"Telegram 發送失敗: {res.text}")
    except Exception as e:
        print(f"連線 Telegram 發生異常: {e}")

def scan_5k_momentum():
    tz_tw = pytz.timezone("Asia/Taipei")
    now_tw = datetime.now(tz_tw)
    print(f"[{now_tw.strftime('%Y-%m-%d %H:%M:%S')}] 啟動 5K 爆量急拉掃描...")

    alerts = []

    for symbol in WATCHLIST:
        try:
            stock = yf.Ticker(symbol)
            # 抓取最近 6 天的 5 分 K 資料 (確保涵蓋完整過去 5 個交易日)
            df = stock.history(period="6d", interval="5m")
            
            if df.empty or len(df) < 30:
                continue

            # 轉換為台灣時區
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC").tz_convert(tz_tw)
            else:
                df.index = df.index.tz_convert(tz_tw)

            # 【關鍵處理】若最後一根 5K 還在跳動未收線（距離現在小於 4.5 分鐘），
            # 則取倒數第二根「剛收完線的完整 5K」來判斷，避免剛換分時成交量太小漏抓
            last_bar_time = df.index[-1]
            if (now_tw - last_bar_time).total_seconds() < 270 and len(df) > 2:
                target_bar = df.iloc[-2]
                prev_bar = df.iloc[-3]
                history_df = df.iloc[:-2]
            else:
                target_bar = df.iloc[-1]
                prev_bar = df.iloc[-2]
                history_df = df.iloc[:-1]

            # 過濾掉歷史資料中成交量為 0 的異常 K 棒，取最近 270 根 5K (約 5 個交易日，每日 54 根 5K)
            valid_history = history_df[history_df["Volume"] > 0].tail(270)
            if len(valid_history) < 20:
                continue

            # 1. 計算最近 5 日的「5K 平均成交量」
            avg_5k_vol = valid_history["Volume"].mean()
            current_5k_vol = target_bar["Volume"]
            vol_ratio = current_5k_vol / avg_5k_vol if avg_5k_vol > 0 else 0

            # 2. 計算這根 5K 的「瞬間漲幅」：包含兩種計算取最大值
            #    - 實體漲幅：(本根收盤 - 本根開盤) / 本根開盤
            #    - 跳空連貫漲幅：(本根收盤 - 上一根收盤) / 上一根收盤
            open_p = target_bar["Open"]
            close_p = target_bar["Close"]
            prev_close_p = prev_bar["Close"]

            bar_body_pct = ((close_p - open_p) / open_p) * 100 if open_p > 0 else 0
            bar_jump_pct = ((close_p - prev_close_p) / prev_close_p) * 100 if prev_close_p > 0 else 0
            instant_pct = max(bar_body_pct, bar_jump_pct)

            bar_time_str = target_bar.name.strftime("%H:%M")
            # 換算成台股習慣的「張數」（1張 = 1000股）
            vol_lots = int(current_5k_vol / 1000)
            avg_lots = int(avg_5k_vol / 1000)

            print(f"[{symbol}] {bar_time_str} | 現價:{close_p:.2f} | 5K瞬間漲幅:{instant_pct:.2f}% | 5K量:{vol_lots}張 (均量:{avg_lots}張, {vol_ratio:.1f}倍)")

            # 【觸發條件】：
            # 1. 單根 5K 成交量 >= 過去 5 日 5K 均量的 3.0 倍
            # 2. 單根 5K 瞬間漲幅 >= 2.0%
            # 3. 必須為紅 K 或平盤上急拉 (收盤 >= 開盤)
            if vol_ratio >= 3.0 and instant_pct >= 2.0 and close_p >= open_p:
                alert_msg = (
                    f"🔥 *【5K 爆量急拉警報】*\n"
                    f"🎯 標的：`{symbol}`\n"
                    f"⏰ K棒時間：`{bar_time_str}`\n"
                    f"🚀 5K瞬間急拉：`+{instant_pct:.2f}%`\n"
                    f"💥 5K爆量倍數：`{vol_ratio:.1f} 倍` (5日5K均量)\n"
                    f"📊 本根成交量：`{vol_lots:,} 張` (均量 `{avg_lots:,} 張`)\n"
                    f"💰 當前價格：`{close_p:.2f}`"
                )
                alerts.append(alert_msg)

        except Exception as e:
            print(f"處理 {symbol} 時發生錯誤: {e}")

    if alerts:
        header = f"⚡ *盤中 5K 動能雷達觸發 ({now_tw.strftime('%H:%M')})*\n\n"
        send_telegram(header + "\n\n".join(alerts))
        print("✅ 已推播警報至 Telegram！")
    else:
        print("掃描完畢，本時段無符合條件之標的。")

if __name__ == "__main__":
    scan_5k_momentum()

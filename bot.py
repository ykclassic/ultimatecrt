import os
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime

# ============================= CONFIGURATION =============================
APP_NAME = "UltimateCRT Bot"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK")
BASE_URL = "https://sapi.xt.com/v4/public/kline"
SYMBOLS = ["BTC_USDT", "ETH_USDT", "SOL_USDT", "BNB_USDT"]
STATE_FILE = "last_signal.json"

# ============================= INDICATORS =============================
def ema(series, period): 
    return series.ewm(span=period, adjust=False).mean()

def detect_fvg(df):
    bullish = df['low'] > df['high'].shift(2)
    bearish = df['high'] < df['low'].shift(2)
    return df['low'].where(bullish).ffill(), df['high'].where(bearish).ffill()

def detect_ob(df):
    bull_ob = (df['close'] > df['open']) & (df['close'].shift(-1) > df['high'])
    bear_ob = (df['close'] < df['open']) & (df['close'].shift(-1) < df['low'])
    return df['low'][bull_ob.shift(1).fillna(False)].ffill(), df['high'][bear_ob.shift(1).fillna(False)].ffill()

def get_swing_levels(df, length=5):
    # Returns the most recent local swing low and swing high
    recent_low = df['low'].rolling(window=length).min().iloc[-1]
    recent_high = df['high'].rolling(window=length).max().iloc[-1]
    return recent_low, recent_high

# ============================= LOGIC & NOTIFICATIONS =============================
def get_last_signal():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_last_signal(sym, timestamp, direction):
    data = get_last_signal()
    data[sym] = {"timestamp": str(timestamp), "direction": direction}
    with open(STATE_FILE, "w") as f:
        json.dump(data, f)

def send_discord_alert(content):
    if not DISCORD_WEBHOOK_URL: return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": content}, timeout=10)
    except Exception as e:
        print(f"Discord Alert Failed: {e}")

def fetch_klines(symbol: str):
    params = {"symbol": symbol, "interval": "5min", "limit": 300}
    try:
        resp = requests.get(BASE_URL, params=params, timeout=15)
        data = resp.json()
        if data.get("rc") == 0:
            return pd.DataFrame(data["data"], columns=["timestamp", "open", "high", "low", "close", "volume"])
    except:
        return pd.DataFrame()
    return pd.DataFrame()

def analyze_symbol(sym):
    df = fetch_klines(sym)
    if df.empty or len(df) < 200: return

    df = df.astype({'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})
    last_candle_time = df['timestamp'].iloc[-1]
    history = get_last_signal()
    
    # Calculate Core Indicators
    close = df['close']
    ema200 = ema(close, 200)
    fvg_up, fvg_down = detect_fvg(df)
    ob_bull, ob_bear = detect_ob(df)
    vol_avg = df['volume'].rolling(20).mean()
    swing_l, swing_h = get_swing_levels(df)

    p, v = close.iloc[-1], df['volume'].iloc[-1]
    
    # Institutional Confluence
    vol_spike = v > vol_avg.iloc[-1] * 1.5
    fvg_b = not np.isnan(fvg_up.iloc[-1]) and p >= fvg_up.iloc[-1]
    ob_b = not np.isnan(ob_bull.iloc[-1]) and p >= ob_bull.iloc[-1]
    fvg_s = not np.isnan(fvg_down.iloc[-1]) and p <= fvg_down.iloc[-1]
    ob_s = not np.isnan(ob_bear.iloc[-1]) and p <= ob_bear.iloc[-1]

    direction = None
    sl, tp = 0, 0

    # BULLISH Logic
    if p > ema200.iloc[-1] and vol_spike and (fvg_b and ob_b):
        direction = "BULLISH"
        sl = swing_l - (p * 0.0005) # SL at Swing Low with small buffer
        risk = p - sl
        tp = p + (risk * 2.5) # 1:2.5 RR

    # BEARISH Logic
    elif p < ema200.iloc[-1] and vol_spike and (fvg_s and ob_s):
        direction = "BEARISH"
        sl = swing_h + (p * 0.0005) # SL at Swing High with small buffer
        risk = sl - p
        tp = p - (risk * 2.5) # 1:2.5 RR

    # Deduplication and Alerting
    if direction:
        prev = history.get(sym, {})
        if prev.get("timestamp") != str(last_candle_time) or prev.get("direction") != direction:
            emoji = "🟢" if direction == "BULLISH" else "🔴"
            msg = (
                f"🚀 **{APP_NAME} SIGNAL**\n"
                f"**Symbol:** {sym}\n"
                f"**Direction:** {direction} {emoji}\n"
                f"**Entry:** {p:,.4f}\n"
                f"**Stop Loss:** {sl:,.4f}\n"
                f"**Take Profit:** {tp:,.4f}\n"
                f"**RR Ratio:** 1:2.5\n"
                f"**Time:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
            )
            send_discord_alert(msg)
            save_last_signal(sym, last_candle_time, direction)

if __name__ == "__main__":
    for symbol in SYMBOLS:
        analyze_symbol(symbol)

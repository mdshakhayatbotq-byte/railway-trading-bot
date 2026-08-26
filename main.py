
# advanced_signal_bot_live_with_chart.py
import requests
import pandas as pd
import numpy as np
import time
import json
import websocket
import threading
from datetime import datetime
import logging
import matplotlib.pyplot as plt
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = "8222223746:AAGSX4HrPdDlvRBr7Gu2cjQ3sng72EioqE4"
TELEGRAM_CHAT_ID = "-1003885154692"

# Crypto symbols (24/7 live)
CRYPTO_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

class LiveDataManager:
    def __init__(self):
        self.candles = {}
        self.candle_data = {}
        self.running = True
        self.ws = None
        self.symbols = []
        self.last_price_log = {}
        
    def start_stream(self, symbols):
        self.symbols = symbols
        
        for symbol in symbols:
            self.candles[symbol] = []
            self.last_price_log[symbol] = 0
            self.candle_data[symbol] = {
                'current_minute': None,
                'ticks': [],
                'open': None, 'high': None, 'low': None, 'close': None
            }
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                
                if 'e' in data and data['e'] == 'trade':
                    symbol = data['s']
                    price = float(data['p'])
                    self.process_tick(symbol, price, datetime.now())
                    
                elif 'ping' in data:
                    ws.send(json.dumps({"pong": data['ping']}))
                    
            except Exception as e:
                pass
        
        def on_error(ws, error):
            logger.error(f"WebSocket error: {error}")
        
        def on_close(ws, close_status_code, close_msg):
            logger.warning("WebSocket closed. Reconnecting in 5s...")
            time.sleep(5)
            if self.running:
                self.start_stream(symbols)
        
        def on_open(ws):
            logger.info("✅ Binance WebSocket connected")
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [f"{s.lower()}@trade" for s in symbols],
                "id": 1
            }
            ws.send(json.dumps(subscribe_msg))
            logger.info(f"📡 Subscribed to: {', '.join(symbols)}")
        
        socket_url = "wss://stream.binance.com:9443/ws"
        self.ws = websocket.WebSocketApp(socket_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
        
        wst = threading.Thread(target=self.ws.run_forever, daemon=True)
        wst.start()
    
    def process_tick(self, symbol, price, timestamp):
        current_minute = timestamp.replace(second=0, microsecond=0)
        data = self.candle_data.get(symbol)
        if not data:
            return
        
        if data['current_minute'] != current_minute:
            if data['ticks'] and data['current_minute'] is not None:
                candle = self.build_candle(symbol, data['current_minute'], data['ticks'])
                if candle:
                    self.candles[symbol].append(candle)
                    if len(self.candles[symbol]) > 200:
                        self.candles[symbol] = self.candles[symbol][-200:]
            
            data['current_minute'] = current_minute
            data['ticks'] = []
            data['open'] = price
            data['high'] = price
            data['low'] = price
        
        data['ticks'].append({'price': price, 'time': timestamp})
        data['close'] = price
        data['high'] = max(data['high'], price)
        data['low'] = min(data['low'], price)
        
        current_second = timestamp.second
        if self.last_price_log.get(symbol, 0) != current_second:
            self.last_price_log[symbol] = current_second
            logger.info(f"💚 {symbol}: ${price:.2f}")
    
    def build_candle(self, symbol, minute_time, ticks):
        if not ticks:
            return None
        prices = [t['price'] for t in ticks]
        return {
            'symbol': symbol,
            'timestamp': minute_time,
            'open': ticks[0]['price'],
            'high': max(prices),
            'low': min(prices),
            'close': ticks[-1]['price'],
            'volume': len(ticks)
        }
    
    def get_dataframe(self, symbol):
        if symbol not in self.candles or len(self.candles[symbol]) < 30:
            return None
        df = pd.DataFrame(self.candles[symbol])
        return df.sort_values('timestamp')
    
    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()


class AdvancedSignalBot:
    def __init__(self, symbols=CRYPTO_SYMBOLS):
        self.last_signal = {}
        self.trade = {}
        self.beta_weights = None
        self.symbols = symbols
        self.data_manager = LiveDataManager()
        
        for symbol in symbols:
            self.last_signal[symbol] = None
            self.trade[symbol] = 0
        
        self.data_manager.start_stream(symbols)
        logger.info("✅ Advanced Signal Bot initialized with Binance WebSocket")
    
    def calculate_ema(self, data, period):
        if len(data) < period:
            return data
        return data.ewm(span=period, adjust=False).mean()
    
    def calculate_beta_filter(self, data, length=50, alpha=3.0, beta=3.0):
        if len(data) < length:
            return data
        
        if self.beta_weights is None or len(self.beta_weights) != length:
            weights = []
            for i in range(length):
                x = i / (length - 1) if length > 1 else 0.5
                w = (x ** (alpha - 1)) * ((1 - x) ** (beta - 1))
                weights.append(w)
            weights = np.array(weights)
            weights = weights / weights.sum()
            self.beta_weights = weights
        
        filtered = []
        for i in range(len(data)):
            if i < length - 1:
                filtered.append(data.iloc[i])
            else:
                window = data.iloc[i-length+1:i+1].values
                filtered.append(np.sum(window * self.beta_weights))
        return pd.Series(filtered, index=data.index)
    
    def calculate_macd(self, data, fast=12, slow=26, signal=9):
        ema_fast = self.calculate_ema(data, fast)
        ema_slow = self.calculate_ema(data, slow)
        macd = ema_fast - ema_slow
        signal_line = self.calculate_ema(macd, signal)
        hist = macd - signal_line
        return macd, signal_line, hist
    
    def calculate_adx(self, high, low, close, period=14, smoothing=10):
        high_low = high - low
        high_close = np.abs(high - close.shift())
        low_close = np.abs(low - close.shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        
        up_move = high.diff()
        down_move = -low.diff()
        
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
        
        atr = tr.rolling(window=period).mean()
        plus_di = 100 * (pd.Series(plus_dm).rolling(window=period).mean() / atr)
        minus_di = 100 * (pd.Series(minus_dm).rolling(window=period).mean() / atr)
        
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.rolling(window=smoothing).mean()
        return plus_di, minus_di, adx
    
    def calculate_rsi(self, data, period=14):
        if len(data) < period + 1:
            return pd.Series([50] * len(data), index=data.index)
        delta = data.diff()
        gain = delta.where(delta > 0, 0).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    def get_signal(self, df, symbol):
        if df is None or len(df) < 50:
            return None
        
        close = df['close']
        high = df['high']
        low = df['low']
        
        filtered_close = self.calculate_beta_filter(close, length=50, alpha=3.0, beta=3.0)
        
        ema3 = self.calculate_ema(close, 3)
        ema5 = self.calculate_ema(close, 5)
        ema9 = self.calculate_ema(close, 9)
        
        macd_line, signal_line, _ = self.calculate_macd(filtered_close, 12, 26, 9)
        
        plus_di, minus_di, adx = self.calculate_adx(high, low, close, 14, 10)
        
        current_price = close.iloc[-1]
        current_macd = macd_line.iloc[-1]
        current_signal = signal_line.iloc[-1]
        current_plus_di = plus_di.iloc[-1]
        current_minus_di = minus_di.iloc[-1]
        current_adx = adx.iloc[-1]
        current_rsi = self.calculate_rsi(close).iloc[-1]
        
        longcheck = current_plus_di > current_minus_di and current_macd > current_signal
        shortcheck = current_minus_di > current_plus_di and current_signal > current_macd
        
        new_trade = self.trade[symbol]
        
        if self.trade[symbol] == 0 and longcheck:
            new_trade = 1
        elif self.trade[symbol] == 0 and shortcheck:
            new_trade = -1
        elif self.trade[symbol] == 1 and shortcheck:
            new_trade = -1
        elif self.trade[symbol] == -1 and longcheck:
            new_trade = 1
        
        self.trade[symbol] = new_trade
        
        if new_trade == 1 and self.last_signal[symbol] != "BUY":
            self.last_signal[symbol] = "BUY"
            return {
                'signal': 'BUY',
                'symbol': symbol,
                'confidence': 'HIGH' if (longcheck and current_adx > 25) else 'MEDIUM',
                'price': current_price,
                'filtered_price': filtered_close.iloc[-1],
                'ema3': ema3.iloc[-1], 'ema5': ema5.iloc[-1], 'ema9': ema9.iloc[-1],
                'macd': current_macd, 'signal_line': current_signal,
                'rsi': current_rsi, 'adx': current_adx,
                'plus_di': current_plus_di, 'minus_di': current_minus_di,
                'index': len(df) - 1
            }
        elif new_trade == -1 and self.last_signal[symbol] != "SELL":
            self.last_signal[symbol] = "SELL"
            return {
                'signal': 'SELL',
                'symbol': symbol,
                'confidence': 'HIGH' if (shortcheck and current_adx > 25) else 'MEDIUM',
                'price': current_price,
                'filtered_price': filtered_close.iloc[-1],
                'ema3': ema3.iloc[-1], 'ema5': ema5.iloc[-1], 'ema9': ema9.iloc[-1],
                'macd': current_macd, 'signal_line': current_signal,
                'rsi': current_rsi, 'adx': current_adx,
                'plus_di': current_plus_di, 'minus_di': current_minus_di,
                'index': len(df) - 1
            }
        
        return None
    
    def create_chart(self, df, signal_data, signal_index):
        try:
            chart_df = df.tail(100).copy().reset_index(drop=True)
            
            fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(16, 14), 
                                                 gridspec_kw={'height_ratios': [3, 1, 1, 1]},
                                                 dpi=100)
            
            width = 0.6
            up = chart_df[chart_df['close'] >= chart_df['open']]
            down = chart_df[chart_df['close'] < chart_df['open']]
            
            up_color = '#00ff00'
            down_color = '#ff0000'
            
            if not up.empty:
                ax1.bar(up.index, up['close'] - up['open'], width, bottom=up['open'], color=up_color, alpha=0.7)
                ax1.bar(up.index, up['high'] - up['close'], width, bottom=up['close'], color=up_color, alpha=0.7)
                ax1.bar(up.index, up['low'] - up['open'], width, bottom=up['open'], color=up_color, alpha=0.7)
            
            if not down.empty:
                ax1.bar(down.index, down['open'] - down['close'], width, bottom=down['close'], color=down_color, alpha=0.7)
                ax1.bar(down.index, down['high'] - down['open'], width, bottom=down['open'], color=down_color, alpha=0.7)
                ax1.bar(down.index, down['low'] - down['close'], width, bottom=down['close'], color=down_color, alpha=0.7)
            
            if 0 <= signal_index < len(chart_df):
                candle = chart_df.iloc[signal_index]
                signal_y = candle['high'] if signal_data['signal'] == 'BUY' else candle['low']
                marker = '^' if signal_data['signal'] == 'BUY' else 'v'
                color = 'green' if signal_data['signal'] == 'BUY' else 'red'
                ax1.scatter(signal_index, signal_y, marker=marker, color=color, s=200, zorder=5)
            
            ema3 = self.calculate_ema(chart_df['close'], 3)
            ema5 = self.calculate_ema(chart_df['close'], 5)
            ema9 = self.calculate_ema(chart_df['close'], 9)
            
            ax1.plot(chart_df.index, ema3, color='blue', linewidth=1.5, label='EMA3')
            ax1.plot(chart_df.index, ema5, color='orange', linewidth=1.5, label='EMA5')
            ax1.plot(chart_df.index, ema9, color='purple', linewidth=1.5, label='EMA9')
            
            beta_filtered = self.calculate_beta_filter(chart_df['close'])
            ax1.plot(chart_df.index, beta_filtered, color='cyan', linewidth=2, linestyle='--', label='Beta Filter', alpha=0.7)
            
            ax1.set_title(f'{signal_data["symbol"]} - 1 Minute Chart (LIVE WebSocket)', fontsize=14, fontweight='bold')
            ax1.set_ylabel('Price', fontsize=12)
            ax1.legend(loc='upper left')
            ax1.grid(True, alpha=0.3)
            
            rsi = self.calculate_rsi(chart_df['close'])
            ax2.plot(chart_df.index, rsi, color='purple', linewidth=1.5)
            ax2.axhline(y=70, color='red', linestyle='--', alpha=0.5)
            ax2.axhline(y=30, color='green', linestyle='--', alpha=0.5)
            ax2.set_ylabel('RSI', fontsize=12)
            ax2.set_ylim(0, 100)
            ax2.grid(True, alpha=0.3)
            
            macd_line, signal_line, hist = self.calculate_macd(chart_df['close'])
            ax3.bar(chart_df.index, hist, color=['red' if x < 0 else 'green' for x in hist], alpha=0.5)
            ax3.plot(chart_df.index, macd_line, color='blue', linewidth=1.5, label='MACD')
            ax3.plot(chart_df.index, signal_line, color='orange', linewidth=1.5, label='Signal')
            ax3.set_ylabel('MACD', fontsize=12)
            ax3.legend(loc='upper left')
            ax3.grid(True, alpha=0.3)
            
            plus_di, minus_di, adx = self.calculate_adx(chart_df['high'], chart_df['low'], chart_df['close'])
            ax4.plot(chart_df.index, plus_di, color='green', linewidth=1.5, label='+DI')
            ax4.plot(chart_df.index, minus_di, color='red', linewidth=1.5, label='-DI')
            ax4.plot(chart_df.index, adx, color='blue', linewidth=1.5, label='ADX')
            ax4.axhline(y=25, color='orange', linestyle='--', alpha=0.5)
            ax4.set_ylabel('ADX/DI', fontsize=12)
            ax4.legend(loc='upper left')
            ax4.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            buf.seek(0)
            plt.close()
            
            return buf
            
        except Exception as e:
            logger.error(f"Chart error: {e}")
            return None
    
    def send_telegram_signal(self, signal_data, chart_buffer):
        try:
            emoji = "🟢" if signal_data['signal'] == 'BUY' else "🔴"
            action = "LONG 🚀" if signal_data['signal'] == 'BUY' else "SHORT 📉"
            
            message = f"""{emoji} <b>LIVE TRADING SIGNAL (With Beta Filter)</b> {emoji}

<b>Symbol:</b> {signal_data['symbol']} ₿
<b>Market:</b> CRYPTO (24/7 Live)
<b>Action:</b> {action}
<b>Confidence:</b> {signal_data['confidence']}
<b>Price:</b> <code>${signal_data['price']:.2f}</code>
<b>Filtered Price:</b> <code>${signal_data['filtered_price']:.2f}</code>

📊 <b>Technical Analysis:</b>
━━━━━━━━━━━━━━━━━━━━━
<b>EMA System:</b>
• EMA3: <code>{signal_data['ema3']:.2f}</code>
• EMA5: <code>{signal_data['ema5']:.2f}</code>
• EMA9: <code>{signal_data['ema9']:.2f}</code>

<b>Momentum (Beta Filtered):</b>
• MACD: <code>{signal_data['macd']:.5f}</code>
• Signal: <code>{signal_data['signal_line']:.5f}</code>
• RSI: <code>{signal_data['rsi']:.1f}</code>

<b>Trend Strength:</b>
• ADX: <code>{signal_data['adx']:.1f}</code>
• +DI: <code>{signal_data['plus_di']:.1f}</code>
• -DI: <code>{signal_data['minus_di']:.1f}</code>

<b>Signal Logic:</b>
• Condition: +DI > -DI AND MACD > Signal
• Beta Filter: ✅ Active
• Data Source: Binance WebSocket (Real-time)

⏰ <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

⚠️ <i>Risk Management: Use 2% stop loss</i>"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            files = {'photo': ('chart.png', chart_buffer, 'image/png')}
            data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': message, 'parse_mode': 'HTML'}
            
            response = requests.post(url, files=files, data=data, timeout=30)
            
            if response.status_code == 200:
                logger.info(f"✅ Signal sent with chart: {signal_data['symbol']} - {signal_data['signal']}")
            else:
                logger.error(f"Telegram error: {response.text}")
                
        except Exception as e:
            logger.error(f"Send error: {e}")
    
    def run(self):
        logger.info("="*60)
        logger.info("🚀 ADVANCED SIGNAL BOT - BINANCE WEBSOCKET (LIVE)")
        logger.info(f"📊 Active Symbols: {self.symbols}")
        logger.info("🤖 Strategy: ADX + MACD + Beta Filter")
        logger.info("📸 Chart generation: ENABLED")
        logger.info("📡 Data Source: Binance WebSocket (Real-time, No API Key)")
        logger.info("💚 Live data streaming 24/7")
        logger.info("="*60)
        
        try:
            startup = f"🚀 Advanced Signal Bot Started (With Charts!)\n\nSymbols: {', '.join(self.symbols)}\nData: Live from Binance WebSocket"
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                         json={'chat_id': TELEGRAM_CHAT_ID, 'text': startup})
        except:
            pass
        
        last_minute_checked = {}
        
        try:
            while True:
                current_min = datetime.now().strftime("%Y-%m-%d %H:%M")
                
                for symbol in self.symbols:
                    df = self.data_manager.get_dataframe(symbol)
                    
                    if df is not None and len(df) >= 50:
                        if last_minute_checked.get(symbol) != current_min:
                            signal = self.get_signal(df, symbol)
                            if signal:
                                chart = self.create_chart(df, signal, signal['index'])
                                if chart:
                                    self.send_telegram_signal(signal, chart)
                            last_minute_checked[symbol] = current_min
                
                time.sleep(2)
                
        except KeyboardInterrupt:
            logger.info("🛑 Bot stopped")
            self.data_manager.stop()
        except Exception as e:
            logger.error(f"Error: {e}")
            self.data_manager.stop()


if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║     ADVANCED SIGNAL BOT - BINANCE WEBSOCKET             ║
    ║     Live Data | Charts | ADX + MACD + Beta Filter       ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    bot = AdvancedSignalBot(symbols=CRYPTO_SYMBOLS)
    bot.run()

# -*- coding: utf-8 -*-
"""
Кнопка бабло.py
Фінальна версія з виправленням журналу угод та RR 1:2.
"""
import os
import json
import asyncio
import logging
from datetime import datetime, time, UTC
from decimal import Decimal, ROUND_DOWN
import pandas as pd
import ta
import pytz
from dotenv import load_dotenv
from binance import AsyncClient, exceptions

try:
    from binance.streams import BinanceSocketManager
except ImportError:
    from binance import BinanceSocketManager

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# ----------------- LOAD CONFIG -----------------
load_dotenv()

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

CFG = {
    # --- Нові параметри для стратегії "Пробій та Ретест" ---
    "RANGE_WINDOW": 45,  # Кількість свічок для визначення каналу (напр. 45 хвилин)
    "RETEST_CONFIRM_CANDLES": 3,  # Скільки свічок чекати після ретесту для підтвердження
    # --- Загальні параметри ---
    "PRIMARY_TF": "1m", "LOOKBACK": 200, "MACD_FAST": 12, "MACD_SLOW": 26, "MACD_SIGNAL": 9, "ATR_WINDOW": 14,
    "TREND_TIMEFRAME": "15m", "TREND_EMA_PERIOD": 50,
    # --- Основні налаштування ризику ---
    "LEVERAGE": 5, "SL_ATR_MULT": 1.5,
    "RISK_PERCENT_OF_BALANCE": 2.0,
    "MAX_POSITION_SIZE_USDT": 150.0,
    "DRY_RUN": True,
    "RISK_REWARD_RATIO": 2.0,  # <--- ВСТАНОВЛЕНО 1 до 2
    # --- Загальні налаштування бота ---
    "TOP_N_SYMBOLS_BY_VOLUME": 30, "SYMBOL_REFRESH_HOURS": 4, "SYMBOL_BLACKLIST": ["USDCUSDT", "BTCDOMUSDT"],
    "DAILY_REPORT_TIME": "09:00", "DAILY_LOSS_LIMIT_USDT": 3.0,
}

KYIV_TZ = pytz.timezone('Europe/Kiev')
STATE_FILE = "bot_state.json"
TRADES_LOG_FILE = "trades_log.csv"
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("scalp_bot")

BOT_STATE = {
    "todays_pnl": 0.0, "last_trade_date_kyiv": datetime.now(KYIV_TZ).date().isoformat(), "trading_enabled": True,
    "daily_limit_triggered": False, "active_symbols": [], "open_symbols": [],
    "open_positions_data": {}, "breakout_levels": {},
}

_api_semaphore: asyncio.Semaphore | None = None
_symbol_info_cache = {}
app: Application | None = None


# ----------------- UTILS & HELPERS -----------------

def save_state_to_file():
    with open(STATE_FILE, 'w') as f: json.dump(BOT_STATE, f, indent=4)
    logger.debug("Стан бота збережено.")


def load_state_from_file():
    global BOT_STATE
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                loaded_state = json.load(f)
                for key, value in BOT_STATE.items():
                    if key not in loaded_state:
                        loaded_state[key] = value
                BOT_STATE.update(loaded_state)
            logger.info("Стан бота завантажено з файлу.")
            check_and_reset_pnl()
    except Exception as e:
        logger.error(f"Не вдалося завантажити стан з файлу: {e}")


def log_trade_to_csv(trade_data: dict):
    df = pd.DataFrame([trade_data])
    file_exists = os.path.isfile(TRADES_LOG_FILE)
    df.to_csv(TRADES_LOG_FILE, mode='a', header=not file_exists, index=False)
    logger.info(f"Угоду по {trade_data['symbol']} записано в журнал.")


def _dec(v: float | str) -> Decimal: return Decimal(str(v))


def round_down_to_step(value: float, step: float) -> float:
    if step == 0: return value
    d_val, d_step = _dec(value), _dec(step)
    result = (d_val / d_step).to_integral_value(rounding=ROUND_DOWN) * d_step
    return float(result)


async def safe_api_call(func, *args, retries=3, delay=0.8, **kwargs):
    global _api_semaphore
    if _api_semaphore is None: _api_semaphore = asyncio.Semaphore(10)
    async with _api_semaphore:
        for attempt in range(1, retries + 1):
            try:
                return await func(*args, **kwargs)
            except exceptions.BinanceAPIException as e:
                logger.warning(f"Помилка Binance API, спроба {attempt}: {e}")
                return e
            except Exception as e:
                logger.warning(f"Загальна помилка API, спроба {attempt}: {e}")
                await asyncio.sleep(delay * attempt)
        return None


def check_and_reset_pnl():
    global BOT_STATE
    today_kyiv_str = datetime.now(KYIV_TZ).date().isoformat()
    if BOT_STATE["last_trade_date_kyiv"] != today_kyiv_str:
        logger.info(f"Новий день ({today_kyiv_str}). Скидаю денний PnL та вмикаю торгівлю.")
        BOT_STATE.update({"todays_pnl": 0.0, "last_trade_date_kyiv": today_kyiv_str, "trading_enabled": True,
                          "daily_limit_triggered": False})
        save_state_to_file()
        if app: asyncio.create_task(
            app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="☀️ **Доброго ранку!** Денний ліміт збитків скинуто.",
                                 parse_mode='Markdown'))


# ----------------- WEBSOCKET HANDLER -----------------

async def handle_socket_message(msg):
    global BOT_STATE
    try:
        if msg.get('e') == 'ORDER_TRADE_UPDATE':
            order_data = msg.get('o', {})
            if order_data.get('X') == 'FILLED' and float(order_data.get('rp', 0)) != 0:
                symbol = order_data.get('s')
                position_data = BOT_STATE["open_positions_data"].pop(symbol, None)
                if position_data:
                    trade_log = {"timestamp_utc": datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S'), "symbol": symbol,
                                 "side": position_data['side'], "entry_price": position_data['entry_price'],
                                 "exit_price": float(order_data.get('p')), "pnl_usdt": float(order_data.get('rp')),
                                 "reason": "SL/TP"}
                    log_trade_to_csv(trade_log)
                else:
                    logger.warning(
                        f"Отримано сигнал про закриття {symbol}, але даних про вхід НЕ ЗНАЙДЕНО в пам'яті бота!")
                if symbol in BOT_STATE["open_symbols"]:
                    BOT_STATE["open_symbols"].remove(symbol)
                check_and_reset_pnl()
                pnl = float(order_data.get('rp'))
                BOT_STATE["todays_pnl"] += pnl
                save_state_to_file()
                pnl_emoji = "✅" if pnl >= 0 else "🔻"
                closing_msg = (
                    f"{pnl_emoji} **Позицію {order_data.get('S')} {symbol} закрито.**\n\n📈 **Прибуток/Збиток (PnL):** `{pnl:.2f} USDT`\n📊 **Сукупний PnL за сьогодні:** `{BOT_STATE['todays_pnl']:.2f} USDT`")
                logger.info(f"Позицію закрито. Символ: {symbol}, PnL: {pnl}, Денний PnL: {BOT_STATE['todays_pnl']:.2f}")
                if app: await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=closing_msg, parse_mode='Markdown')
                if BOT_STATE["todays_pnl"] < -CFG["DAILY_LOSS_LIMIT_USDT"] and not BOT_STATE["daily_limit_triggered"]:
                    BOT_STATE.update({"trading_enabled": False, "daily_limit_triggered": True})
                    save_state_to_file()
                    stop_msg = (
                        f"🛑 **УВАГА: Денний ліміт збитків (-{CFG['DAILY_LOSS_LIMIT_USDT']:.2f} USDT) досягнуто!**\n\nТоргівлю зупинено до наступного дня.")
                    logger.warning(
                        f"Досягнуто денний ліміт збитків! Зупиняю торгівлю. Денний PnL: {BOT_STATE['todays_pnl']:.2f}")
                    if app and 'trading_task' in app.bot_data and not app.bot_data['trading_task'].done():
                        app.bot_data['trading_task'].cancel()
                        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=stop_msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Помилка в обробнику WebSocket: {e}")


async def start_websocket_listener():
    client = None
    while True:
        try:
            client = await create_client()
            bsm = BinanceSocketManager(client)
            logger.info("WebSocket слухач запущений.")
            async with bsm.user_socket() as socket:
                while True:
                    msg = await socket.recv()
                    await handle_socket_message(msg)
        except Exception as e:
            logger.error(f"Критична помилка WebSocket: {e}. Перепідключення через 10 секунд...")
        finally:
            if client: await client.close_connection()
            await asyncio.sleep(10)


# ----------------- BINANCE API FUNCTIONS -----------------

async def create_client() -> AsyncClient:
    return await AsyncClient.create(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)


async def set_leverage_and_margin_type(client: AsyncClient, symbol: str):
    await safe_api_call(client.futures_change_margin_type, symbol=symbol, marginType='ISOLATED')
    await safe_api_call(client.futures_change_leverage, symbol=symbol, leverage=CFG["LEVERAGE"])


async def get_top_volume_symbols(client: AsyncClient) -> list[str]:
    tickers = await safe_api_call(client.futures_ticker)
    if not tickers or not isinstance(tickers, list): return []
    usdt_pairs = [t for t in tickers if t['symbol'].endswith('USDT') and t['symbol'] not in CFG["SYMBOL_BLACKLIST"]]
    sorted_pairs = sorted(usdt_pairs, key=lambda x: float(x['quoteVolume']), reverse=True)
    return [p['symbol'] for p in sorted_pairs[:CFG["TOP_N_SYMBOLS_BY_VOLUME"]]]


async def get_symbol_info(client: AsyncClient, symbol: str) -> dict | None:
    if symbol in _symbol_info_cache: return _symbol_info_cache[symbol]
    data = await safe_api_call(client.futures_exchange_info)
    if not data or not isinstance(data, dict): return None
    for item in data.get('symbols', []):
        if item['symbol'] == symbol:
            info = {'price_precision': int(item['pricePrecision']),
                    'quantity_precision': int(item['quantityPrecision']),
                    'min_qty': float(item['filters'][1]['minQty']),
                    'price_tick_size': float(item['filters'][0]['tickSize']),
                    'qty_step_size': float(item['filters'][1]['stepSize'])}
            _symbol_info_cache[symbol] = info
            return info
    return None


async def fetch_klines_df(client: AsyncClient, symbol: str, interval: str, limit: int) -> pd.DataFrame:
    raw = await safe_api_call(client.futures_klines, symbol=symbol, interval=interval, limit=limit)
    if not raw or not isinstance(raw, list): return pd.DataFrame()

    # Перевірка на достатню кількість даних перед створенням DataFrame
    if len(raw) < limit:
        logger.debug(f"Недостатньо історії для {symbol}. Отримано {len(raw)} з {limit} свічок.")
        return pd.DataFrame()

    cols = ["open_time", "open", "high", "low", "close", "volume", "close_time", "qav", "trades", "tbbav", "tbqav",
            "ignore"]
    df = pd.DataFrame(raw, columns=cols, dtype=float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    return df.set_index("open_time")


async def get_futures_balance(client: AsyncClient) -> float:
    balance_info = await safe_api_call(client.futures_account_balance)
    if not balance_info or not isinstance(balance_info, list): return 0.0
    usdt_balance = next((item for item in balance_info if item['asset'] == 'USDT'), None)
    return float(usdt_balance['availableBalance']) if usdt_balance else 0.0


# ----------------- TRADING STRATEGY & LOGIC -----------------

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    required_length = max(CFG["TREND_EMA_PERIOD"], CFG["MACD_SLOW"], CFG["RANGE_WINDOW"])
    if len(df) < required_length:
        return pd.DataFrame()

    macd = ta.trend.MACD(df["close"], window_slow=CFG["MACD_SLOW"], window_fast=CFG["MACD_FAST"],
                         window_sign=CFG["MACD_SIGNAL"])
    df["macd"], df["macd_signal"] = macd.macd(), macd.macd_signal()
    df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=CFG["ATR_WINDOW"])
    df[f"ema_{CFG['TREND_EMA_PERIOD']}"] = ta.trend.ema_indicator(df["close"], window=CFG['TREND_EMA_PERIOD'])
    return df.dropna()


def find_breakout_and_retest_signal(df: pd.DataFrame, trend: str, current_price: float, symbol: str) -> str | None:
    if len(df) < CFG["RANGE_WINDOW"] + 1: return None

    last_candle = df.iloc[-1]
    retest_data = BOT_STATE.get("breakout_levels", {}).get(symbol)

    if retest_data:
        retest_level = retest_data["level"]
        retest_side = retest_data["side"]

        is_retesting_now = False
        if retest_side == "LONG" and current_price <= retest_level * 1.001 and current_price >= retest_level:
            is_retesting_now = True
        elif retest_side == "SHORT" and current_price >= retest_level * 0.999 and current_price <= retest_level:
            is_retesting_now = True

        if is_retesting_now:
            if (retest_side == "LONG" and last_candle["macd"] > last_candle["macd_signal"]) or \
                    (retest_side == "SHORT" and last_candle["macd"] < last_candle["macd_signal"]):
                logger.info(f"✅ ПІДТВЕРДЖЕННЯ РЕТЕСТУ для {symbol} {retest_side} на рівні {retest_level}")
                BOT_STATE["breakout_levels"].pop(symbol, None)
                return retest_side

        if (retest_side == "LONG" and current_price > retest_level * 1.01) or \
                (retest_side == "SHORT" and current_price < retest_level * 0.99):
            logger.info(f"Скасовано очікування ретесту для {symbol}, ціна пішла далеко.")
            BOT_STATE["breakout_levels"].pop(symbol, None)
        return None

    recent_df = df.iloc[-(CFG["RANGE_WINDOW"] + 1):-1]
    range_high = recent_df['high'].max()
    range_low = recent_df['low'].min()

    if trend == "UP" and last_candle['close'] > range_high:
        logger.info(f"🔥 ПРОБІЙ ВГОРУ для {symbol} на рівні {range_high}")
        BOT_STATE["breakout_levels"][symbol] = {"side": "LONG", "level": range_high}
        return None

    if trend == "DOWN" and last_candle['close'] < range_low:
        logger.info(f"💥 ПРОБІЙ ВНИЗ для {symbol} на рівні {range_low}")
        BOT_STATE["breakout_levels"][symbol] = {"side": "SHORT", "level": range_low}
        return None

    return None


def calculate_sl_price(side: str, entry: float, atr: float) -> float:
    return entry - CFG["SL_ATR_MULT"] * atr if side == "LONG" else entry + CFG["SL_ATR_MULT"] * atr


def calculate_tp_price(side: str, entry: float, sl: float, rr_ratio: float) -> float:
    return entry + abs(entry - sl) * rr_ratio if side == "LONG" else entry - abs(entry - sl) * rr_ratio


def calculate_quantity(entry: float, sl: float, risk: float, info: dict) -> float:
    if abs(entry - sl) == 0: return 0.0
    return round_down_to_step(risk / abs(entry - sl), info['qty_step_size'])


async def trade_symbol(client: AsyncClient, symbol: str, balance: float):
    if symbol in BOT_STATE["open_symbols"]: return
    if not BOT_STATE["trading_enabled"]: return
    try:
        current_price_ticker = await safe_api_call(client.futures_ticker, symbol=symbol)
        if not current_price_ticker or not isinstance(current_price_ticker, dict): return
        current_price = float(current_price_ticker['lastPrice'])

        df_trend = await fetch_klines_df(client, symbol, CFG["TREND_TIMEFRAME"],
                                         CFG["TREND_EMA_PERIOD"] + 50)  # Запас для EMA
        if df_trend.empty: return
        df_trend = add_indicators(df_trend)
        if df_trend.empty: return

        trend_ema = df_trend.iloc[-1][f"ema_{CFG['TREND_EMA_PERIOD']}"]
        is_uptrend = current_price > trend_ema
        is_downtrend = current_price < trend_ema
        trend_direction = "UP" if is_uptrend else "DOWN"

        df1 = add_indicators(await fetch_klines_df(client, symbol, CFG["PRIMARY_TF"], CFG["LOOKBACK"]))
        if df1.empty: return

        logger.info(f"Перевірка {symbol}: Тренд={trend_direction}")

        signal = find_breakout_and_retest_signal(df1, trend_direction, current_price, symbol)
        if not signal: return

        positions = await safe_api_call(client.futures_position_information, symbol=symbol)
        if positions and isinstance(positions, list) and float(positions[0].get('positionAmt', 0)) != 0: return

        info = await get_symbol_info(client, symbol)
        if not info: return

        actual_entry_price = current_price
        risk_usdt = balance * (CFG["RISK_PERCENT_OF_BALANCE"] / 100.0)
        atr_value = float(df1.iloc[-1]["atr"])

        sl_price = calculate_sl_price(signal, actual_entry_price, atr_value)
        tp_price = calculate_tp_price(signal, actual_entry_price, sl_price, CFG["RISK_REWARD_RATIO"])
        quantity = calculate_quantity(actual_entry_price, sl_price, risk_usdt, info)

        if quantity < info['min_qty']:
            logger.info(
                f"Пропуск {symbol}: розрах. к-сть ({quantity}) < мін. ({info['min_qty']}). Ризик USDT: {risk_usdt:.2f}")
            return

        notional_size = quantity * actual_entry_price
        if notional_size > CFG["MAX_POSITION_SIZE_USDT"]:
            logger.info(
                f"Пропуск {symbol}: розрах. позиція ({notional_size:.2f} USDT) перевищує макс. ліміт ({CFG['MAX_POSITION_SIZE_USDT']} USDT).")
            return

        sl_price = round_down_to_step(sl_price, info['price_tick_size'])
        tp_price = round_down_to_step(tp_price, info['price_tick_size'])

        msg = (f"🔮 **Сигнал: {signal} {symbol} (Пробій та Ретест)**\n\n"
               f"➡️ **Вхід:** `{actual_entry_price:.{info['price_precision']}f}`\n"
               f"🛡️ **Stop Loss:** `{sl_price:.{info['price_precision']}f}`\n"
               f"🎯 **Take Profit:** `{tp_price:.{info['price_precision']}f}` (RR: 1:{CFG['RISK_REWARD_RATIO']})\n"
               f"📦 **Кількість:** `{quantity}` (Ризик: {risk_usdt:.2f} USDT)")
        logger.info(
            f"Сигнал: {signal} {symbol} | Вхід: {actual_entry_price} | SL: {sl_price} | TP: {tp_price} | К-сть: {quantity}")
        if app: await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg, parse_mode='Markdown')

        if not CFG["DRY_RUN"]:
            pos_side = "BUY" if signal == "LONG" else "SELL"
            close_side = "SELL" if signal == "LONG" else "BUY"
            order_batch = [
                {'symbol': symbol, 'side': pos_side, 'type': 'MARKET', 'quantity': str(quantity)},
                {'symbol': symbol, 'side': close_side, 'type': 'STOP_MARKET', 'quantity': str(quantity),
                 'stopPrice': str(sl_price), 'reduceOnly': 'true'},
                {'symbol': symbol, 'side': close_side, 'type': 'TAKE_PROFIT_MARKET', 'quantity': str(quantity),
                 'stopPrice': str(tp_price), 'reduceOnly': 'true'}
            ]

            order_results = await safe_api_call(client.futures_place_batch_order, batchOrders=order_batch)
            logger.info(f"Результат відправки пакетних ордерів для {symbol}: {order_results}")

            if isinstance(order_results, list):
                errors = [res for res in order_results if 'code' in res]
                if not errors:
                    BOT_STATE["open_symbols"].append(symbol)
                    BOT_STATE["open_positions_data"][symbol] = {'entry_price': actual_entry_price, 'side': signal}
                    save_state_to_file()
                    if app: await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                                                       text=f"✅ **Позицію {symbol} відкрито!** SL та TP виставлено.",
                                                       parse_mode='Markdown')
                else:
                    error_msg = f"🔴 **Помилка створення позиції {symbol}:**\n"
                    for e in errors: error_msg += f"`{e.get('msg')}`\n"
                    logger.error(error_msg)
                    if app: await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=error_msg, parse_mode='Markdown')
            else:
                error_msg = f"🔴 **Загальна помилка API при створенні позиції {symbol}:**\n`{order_results}`"
                logger.error(error_msg)
                if app: await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=error_msg, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Критична помилка в trade_symbol для {symbol}: {e}")


async def trading_loop(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Торговий цикл запущено.")
    client = await create_client()
    last_symbol_refresh = None
    while BOT_STATE["trading_enabled"]:
        try:
            current_balance = await get_futures_balance(client)
            if current_balance < 1:
                logger.warning("Баланс занадто малий для торгівлі, пауза 5 хвилин.")
                await asyncio.sleep(300)
                continue

            now = datetime.now(UTC)
            if not last_symbol_refresh or (now - last_symbol_refresh).total_seconds() > CFG[
                "SYMBOL_REFRESH_HOURS"] * 3600:
                symbols = await get_top_volume_symbols(client)
                if symbols:
                    BOT_STATE["active_symbols"] = symbols
                    last_symbol_refresh = now
                    save_state_to_file()
                    if app: await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                                                       text=f"🔄 Список монет оновлено. Топ-{len(symbols)} за об'ємом.")
                    setup_tasks = [set_leverage_and_margin_type(client, symbol) for symbol in symbols]
                    await asyncio.gather(*setup_tasks)

            if not BOT_STATE["active_symbols"]:
                await asyncio.sleep(300);
                continue

            tasks = [trade_symbol(client, symbol, current_balance) for symbol in BOT_STATE["active_symbols"]]
            await asyncio.gather(*tasks)

        except asyncio.CancelledError:
            logger.info("Торговий цикл було скасовано.")
            break
        except Exception as e:
            logger.error(f"Помилка в торговому циклі: {e}")
            await asyncio.sleep(30)

        logger.info(f"Цикл завершено. Перевірено {len(BOT_STATE['active_symbols'])} монет. Пауза 60с...")
        await asyncio.sleep(60)

    logger.info("Торговий цикл зупинено.")
    if client: await client.close_connection()


async def get_balance_message() -> str:
    client = await create_client()
    balance_info = await safe_api_call(client.futures_account_balance)
    await client.close_connection()
    if not balance_info or not isinstance(balance_info, list): return "Не вдалося отримати дані про баланс."
    usdt_balance = next((item for item in balance_info if item['asset'] == 'USDT'), None)
    if usdt_balance:
        return (f"💰 **Баланс:** `{float(usdt_balance['balance']):.2f}` USDT\n"
                f"✅ **Доступно:** `{float(usdt_balance['availableBalance']):.2f}` USDT")
    return "Не знайдено USDT баланс."


async def get_status_message() -> str:
    check_and_reset_pnl()
    status = 'АКТИВНА ✅' if BOT_STATE['trading_enabled'] else 'ЗУПИНЕНО 🛑'
    open_positions_str = f"`{', '.join(BOT_STATE['open_symbols'])}`" if BOT_STATE['open_symbols'] else "Немає"
    return (f"**📊 Статус бота**\n\n"
            f"**Торгівля:** {status}\n"
            f"**PnL сьогодні:** `{BOT_STATE['todays_pnl']:.2f}` USDT\n"
            f"**Активні монети:** `{len(BOT_STATE['active_symbols'])}`\n"
            f"**Відкриті позиції (внутрішньо):** {open_positions_str}")


async def stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_message = update.message or (update.callback_query and update.callback_query.message)
    if not os.path.exists(TRADES_LOG_FILE):
        await effective_message.reply_text("Журнал угод ще порожній.")
        return

    try:
        df = pd.read_csv(TRADES_LOG_FILE)
        if df.empty:
            await effective_message.reply_text("Журнал угод порожній.")
            return
    except pd.errors.EmptyDataError:
        await effective_message.reply_text("Журнал угод порожній.")
        return

    total_pnl = df['pnl_usdt'].sum()
    total_trades = len(df)
    winning_trades = df[df['pnl_usdt'] > 0]
    losing_trades = df[df['pnl_usdt'] < 0]
    win_rate = (len(winning_trades) / total_trades) * 100 if total_trades > 0 else 0
    avg_win = winning_trades['pnl_usdt'].mean() if not winning_trades.empty else 0
    avg_loss = losing_trades['pnl_usdt'].mean() if not losing_trades.empty else 0

    stats_message = (
        f"📈 **Статистика торгівлі**\n\n"
        f"**Загальний PnL:** `{total_pnl:.2f} USDT`\n"
        f"**Всього угод:** `{total_trades}`\n"
        f"**Прибуткових:** `{len(winning_trades)}`\n"
        f"**Збиткових:** `{len(losing_trades)}`\n"
        f"**Вінрейт:** `{win_rate:.1f}%`\n"
        f"**Середній прибуток:** `{avg_win:.2f} USDT`\n"
        f"**Середній збиток:** `{avg_loss:.2f} USDT`"
    )
    await effective_message.reply_text(stats_message, parse_mode='Markdown')


async def send_daily_report(context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=context.job.chat_id,
                                   text="📊 **Щоденний звіт**\n" + await get_balance_message(), parse_mode='Markdown')


async def reset_state_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    BOT_STATE["open_symbols"] = []
    BOT_STATE["open_positions_data"] = {}
    BOT_STATE["breakout_levels"] = {}
    save_state_to_file()
    logger.info("Стан було скинуто командою /reset_state.")
    await update.message.reply_text("✅ Внутрішній стан (відкриті позиції, рівні пробою) успішно скинуто!")


async def _get_menu_keyboard() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("📊 Статус", callback_data="status"),
         InlineKeyboardButton("💰 Баланс", callback_data="balance")],
        [InlineKeyboardButton("📈 Статистика", callback_data="stats"),
         InlineKeyboardButton("🔄 Скинути стан", callback_data="reset_state")],
        [InlineKeyboardButton("▶️ Старт", callback_data="start"),
         InlineKeyboardButton("⏹️ Стоп", callback_data="stop")],
        [InlineKeyboardButton(f"🧪 Dry-Run: {'ON' if CFG['DRY_RUN'] else 'OFF'}", callback_data="toggle_dry")]
    ]
    return InlineKeyboardMarkup(kb)


async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Головне меню:", reply_markup=await _get_menu_keyboard())


async def button_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    if data == "status":
        await q.message.reply_text(await get_status_message(), parse_mode='Markdown')
    elif data == "balance":
        msg = await q.message.reply_text("⏳ Отримую дані про баланс...")
        await msg.edit_text(await get_balance_message(), parse_mode='Markdown')
    elif data == "stats":
        await stats_handler(update, context)
    elif data == "reset_state":
        BOT_STATE["open_symbols"] = []
        BOT_STATE["open_positions_data"] = {}
        BOT_STATE["breakout_levels"] = {}
        save_state_to_file()
        logger.info("Стан було скинуто через меню.")
        await q.edit_message_text("✅ Внутрішній стан скинуто. Меню:", reply_markup=await _get_menu_keyboard())
    elif data == "start":
        check_and_reset_pnl()
        if not BOT_STATE["trading_enabled"]:
            await q.edit_message_text("🔴 Торгівлю зупинено через ліміт збитків.",
                                      reply_markup=await _get_menu_keyboard())
            return
        if 'trading_task' not in context.bot_data or context.bot_data['trading_task'].done():
            context.bot_data['trading_task'] = asyncio.create_task(trading_loop(context))
            await q.edit_message_text("✅ **Торговий цикл запущено!**", parse_mode='Markdown',
                                      reply_markup=await _get_menu_keyboard())
        else:
            await q.answer("❕ Цикл вже працює.", show_alert=True)
    elif data == "stop":
        if 'trading_task' in context.bot_data and not context.bot_data['trading_task'].done():
            context.bot_data['trading_task'].cancel()
            await q.edit_message_text("⏹️ **Торговий цикл зупинено.**", parse_mode='Markdown',
                                      reply_markup=await _get_menu_keyboard())
            await context.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="🛑 **Торгівлю зупинено користувачем**.",
                                           parse_mode='Markdown')
        else:
            await q.answer("❕ Цикл не був запущений.", show_alert=True)
    elif data == "toggle_dry":
        CFG["DRY_RUN"] = not CFG["DRY_RUN"]
        status_text = f"🧪 Режим Dry-Run тепер: **{'ON' if CFG['DRY_RUN'] else 'OFF'}**."
        if not CFG['DRY_RUN']: status_text += "\n\n**УВАГА: РЕАЛЬНІ ОРДЕРИ!**"
        await q.edit_message_text(status_text, parse_mode='Markdown', reply_markup=await _get_menu_keyboard())


async def main():
    global app
    if not all([TELEGRAM_TOKEN, BINANCE_API_KEY, BINANCE_API_SECRET, TELEGRAM_CHAT_ID]):
        logger.error("🛑 Перевірте наявність усіх змінних у .env файлі!")
        return

    load_state_from_file()
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", menu_handler))
    app.add_handler(CommandHandler("menu", menu_handler))
    app.add_handler(CommandHandler("stats", stats_handler))
    app.add_handler(CommandHandler("reset_state", reset_state_handler))
    app.add_handler(CallbackQueryHandler(button_cb))

    try:
        h, m = map(int, CFG["DAILY_REPORT_TIME"].split(':'))
        app.job_queue.run_daily(send_daily_report, time(h, m, tzinfo=KYIV_TZ), chat_id=TELEGRAM_CHAT_ID)
    except (ValueError, KeyError):
        logger.error("Неправильний формат DAILY_REPORT_TIME. Використовуйте 'HH:MM'.")

    logger.info("🚀 Бот запускається...")
    try:
        await app.initialize()
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="✅ **Бот успішно запущено!**\nВикористовуйте /menu.",
                                   parse_mode='Markdown')
        await app.start()

        asyncio.create_task(start_websocket_listener())
        await app.updater.start_polling()
        await asyncio.Event().wait()
    finally:
        if app and app.updater: await app.updater.stop()
        if app: await app.stop()
        logger.info("Бот зупинено.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Процес завершено користувачем.")

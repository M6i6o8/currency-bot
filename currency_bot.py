import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
import os
import json
import sys
import re
from dotenv import load_dotenv
from aiohttp import web
from zoneinfo import ZoneInfo
from collections import Counter

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация Telegram
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

# API ключи
TWELVEDATA_KEY = os.getenv('TWELVEDATA_KEY')

# ===== НАСТРОЙКА ДОСТУПА =====
ALLOWED_USER_IDS = [
    5799391012,  # ТВОЙ ID
]

DEFAULT_MODE = "public"  # public - открыт для всех, private - только для своих
# ============================

if len(sys.argv) > 1:
    mode_arg = sys.argv[1].lower()
    PRIVATE_MODE = (mode_arg == "private")
else:
    PRIVATE_MODE = (DEFAULT_MODE == "private")

# Файлы для хранения данных
USER_ALERTS_FILE = "user_alerts.json"
STATS_FILE = "user_stats.json"

def load_user_alerts():
    """Загружает алерты"""
    if os.path.exists(USER_ALERTS_FILE):
        with open(USER_ALERTS_FILE, 'r', encoding='utf-8') as f:
            alerts = json.load(f)
            
        # Конвертируем старые алерты в новый формат
        for user_id, user_alerts in alerts.items():
            for alert in user_alerts:
                if 'target_price' in alert and 'target' not in alert:
                    alert['target'] = alert['target_price']
                    
        return alerts
    return {}

def save_user_alerts(alerts):
    """Сохраняет пользовательские алерты"""
    with open(USER_ALERTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(alerts, f, indent=2, ensure_ascii=False)

def load_user_stats():
    """Загружает статистику пользователей"""
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_user_stats(stats):
    """Сохраняет статистику пользователей"""
    with open(STATS_FILE, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

def update_user_stats(chat_id, username, first_name, last_name, pair=None, timezone=None):
    """Обновляет статистику пользователя"""
    stats = load_user_stats()
    user_id = str(chat_id)
    
    if user_id not in stats:
        stats[user_id] = {
            'first_seen': datetime.now().isoformat(),
            'username': username,
            'first_name': first_name,
            'last_name': last_name,
            'interactions': 0,
            'alerts_created': 0,
            'alerts_triggered': 0,
            'pairs': [],
            'timezone': 'Europe/Moscow',  # По умолчанию Москва
            'timezone_name': 'Москва (UTC+3)'
        }
    
    stats[user_id]['last_seen'] = datetime.now().isoformat()
    stats[user_id]['interactions'] += 1
    
    if pair:
        stats[user_id]['pairs'].append(pair)
        if len(stats[user_id]['pairs']) > 50:
            stats[user_id]['pairs'] = stats[user_id]['pairs'][-50:]
    
    if timezone:
        stats[user_id]['timezone'] = timezone
        stats[user_id]['timezone_name'] = TIMEZONES.get(timezone, {}).get('name', timezone)
    
    save_user_stats(stats)
    return stats[user_id]

def get_user_timezone(user_id):
    """Возвращает часовой пояс пользователя"""
    stats = load_user_stats()
    user_id = str(user_id)
    if user_id in stats and 'timezone' in stats[user_id]:
        return stats[user_id]['timezone']
    return 'Europe/Moscow'  # По умолчанию Москва

# Глобальные переменные
user_alerts = load_user_alerts()
last_notifications = {}

# Московский часовой пояс для внутренних логов
MSK_TZ = ZoneInfo('Europe/Moscow')

# Словарь доступных часовых поясов с городами
TIMEZONES = {
    'Europe/Kaliningrad': {'name': 'Калининград (UTC+2)', 'offset': 2},
    'Europe/Moscow': {'name': 'Москва (UTC+3)', 'offset': 3},
    'Europe/Samara': {'name': 'Самара (UTC+4)', 'offset': 4},
    'Asia/Yekaterinburg': {'name': 'Екатеринбург (UTC+5)', 'offset': 5},
    'Asia/Omsk': {'name': 'Омск (UTC+6)', 'offset': 6},
    'Asia/Krasnoyarsk': {'name': 'Красноярск (UTC+7)', 'offset': 7},
    'Asia/Irkutsk': {'name': 'Иркутск (UTC+8)', 'offset': 8},
    'Asia/Yakutsk': {'name': 'Якутск (UTC+9)', 'offset': 9},
    'Asia/Vladivostok': {'name': 'Владивосток (UTC+10)', 'offset': 10},
    'Asia/Srednekolymsk': {'name': 'Магадан (UTC+11)', 'offset': 11},
    'Asia/Kamchatka': {'name': 'Камчатка (UTC+12)', 'offset': 12},
    'Europe/London': {'name': 'Лондон (UTC+0)', 'offset': 0},
    'Europe/Berlin': {'name': 'Берлин (UTC+1)', 'offset': 1},
    'America/New_York': {'name': 'Нью-Йорк (UTC-5)', 'offset': -5},
    'America/Chicago': {'name': 'Чикаго (UTC-6)', 'offset': -6},
    'America/Denver': {'name': 'Денвер (UTC-7)', 'offset': -7},
    'America/Los_Angeles': {'name': 'Лос-Анджелес (UTC-8)', 'offset': -8},
}

class CurrencyMonitor:
    def __init__(self):
        self.session = None
        self.last_update_id = 0
        self.alert_states = {}
        self.last_successful_rates = {
            # Валюты
            'EUR/USD': 1.08,
            'GBP/USD': 1.26,
            'USD/JPY': 155.0,
            'USD/RUB': 90.0,
            'EUR/GBP': 0.87,
            
            # Металлы
            'XAU/USD': 5160.0,
            'XAG/USD': 30.0,
            
            # Крипта
            'BTC/USD': 67000.0,
            'ETH/USD': 1950.0,
            'SOL/USD': 84.0,
            'BNB/USD': 610.0,
            'LINK/USD': 8.6,
            'TON/USD': 1.35,
            'XRP/USD': 1.40,
            'DOGE/USD': 0.098,
            'AVAX/USD': 9.1,
            
            # Индексы
            'S&P 500': 5100.0,
            'NASDAQ': 18000.0,
            
            # Товары
            'CORN/USD': 4.50
        }
        
        # Для кэширования индексов
        self.last_indices_update = None
        self.cached_indices = None
    
    def is_user_allowed(self, chat_id):
        if not PRIVATE_MODE:
            return True
        return chat_id in ALLOWED_USER_IDS
    
    def is_admin(self, chat_id):
        """Проверяет, является ли пользователь админом"""
        return str(chat_id) in [str(id) for id in ALLOWED_USER_IDS]
    
    async def get_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def fetch_from_binance(self):
        """Получает курсы криптовалют с Binance"""
        try:
            session = await self.get_session()
            result = {}
            
            symbols = {
                'BTC': 'BTCUSDT',
                'ETH': 'ETHUSDT',
                'SOL': 'SOLUSDT',
                'BNB': 'BNBUSDT',
                'LINK': 'LINKUSDT',
                'TON': 'TONUSDT',
                'XRP': 'XRPUSDT',
                'DOGE': 'DOGEUSDT',
                'AVAX': 'AVAXUSDT'
            }
            
            for coin, symbol in symbols.items():
                try:
                    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
                    async with session.get(url, timeout=5) as response:
                        if response.status == 200:
                            data = await response.json()
                            price = float(data['price'])
                            result[f"{coin}/USD"] = price
                            logger.info(f"Binance {coin}: {price}")
                        await asyncio.sleep(0.1)
                except Exception as e:
                    logger.warning(f"Binance {coin} error: {e}")
                    if f"{coin}/USD" in self.last_successful_rates:
                        result[f"{coin}/USD"] = self.last_successful_rates[f"{coin}/USD"]
            
            return result
        except Exception as e:
            logger.error(f"Binance API error: {e}")
            return None
    
    async def fetch_gold_price(self):
        """Получает цену золота через Gold-API"""
        try:
            session = await self.get_session()
            url = "https://api.gold-api.com/price/XAU"
            
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    price = float(data['price'])
                    
                    if price and price > 1000 and price < 10000:
                        logger.info(f"✅ Золото: ${price:.2f}/унция (источник: Gold-API)")
                        return price
        except Exception as e:
            logger.error(f"Gold-API error: {e}")
        
        return self.last_successful_rates.get('XAU/USD', 5160.0)
    
    async def fetch_silver_price(self):
        """Получает цену серебра через Gold-API"""
        try:
            session = await self.get_session()
            url = "https://api.gold-api.com/price/XAG"
            
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    price = float(data['price'])
                    
                    if price and price > 10 and price < 100:
                        logger.info(f"✅ Серебро: ${price:.2f}/унция")
                        return price
        except Exception as e:
            logger.error(f"Silver API error: {e}")
        
        return self.last_successful_rates.get('XAG/USD', 30.0)
    
    async def fetch_indices(self):
        """Получает значения индексов из нескольких источников"""
        now = datetime.now()
        
        # Если обновляли меньше минуты назад - возвращаем кэш
        if self.last_indices_update and self.cached_indices:
            if (now - self.last_indices_update).total_seconds() < 60:
                logger.info("📊 Индексы из кэша")
                return self.cached_indices
        
        result = {}
        
        # Источник 1: Twelve Data (основной)
        try:
            session = await self.get_session()
            url = f"https://api.twelvedata.com/quote?symbol=SPY,QQQ&apikey={TWELVEDATA_KEY}"
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'SPY' in data and 'close' in data['SPY']:
                        result['S&P 500'] = float(data['SPY']['close'])
                    if 'QQQ' in data and 'close' in data['QQQ']:
                        result['NASDAQ'] = float(data['QQQ']['close'])
                    if result:
                        logger.info("✅ Индексы от Twelve Data")
                        self.cached_indices = result
                        self.last_indices_update = now
                        return result
        except Exception as e:
            logger.warning(f"Twelve Data error: {e}")
        
        # Источник 2: Alpha Vantage (бесплатный демо-ключ)
        try:
            session = await self.get_session()
            
            # S&P 500
            url_spy = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=SPY&apikey=demo"
            async with session.get(url_spy, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'Global Quote' in data and '05. price' in data['Global Quote']:
                        result['S&P 500'] = float(data['Global Quote']['05. price'])
            
            # NASDAQ
            url_qqq = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=QQQ&apikey=demo"
            async with session.get(url_qqq, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'Global Quote' in data and '05. price' in data['Global Quote']:
                        result['NASDAQ'] = float(data['Global Quote']['05. price'])
            
            if result:
                logger.info("✅ Индексы от Alpha Vantage")
                self.cached_indices = result
                self.last_indices_update = now
                return result
        except Exception as e:
            logger.warning(f"Alpha Vantage error: {e}")
        
        # Источник 3: Запасные данные
        logger.warning("⚠️ Использую кэшированные значения индексов")
        return self.cached_indices if self.cached_indices else {
            'S&P 500': self.last_successful_rates.get('S&P 500', 5100.0),
            'NASDAQ': self.last_successful_rates.get('NASDAQ', 18000.0)
        }
    
    async def fetch_corn_price(self):
        """Получает цену кукурузы через Twelve Data"""
        try:
            session = await self.get_session()
            
            url = f"https://api.twelvedata.com/quote?symbol=ZC&apikey={TWELVEDATA_KEY}"
            
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'close' in data:
                        price = float(data['close'])
                        logger.info(f"✅ Кукуруза: ${price:.2f}/бушель")
                        return price
                    elif 'code' in data and data['code'] == 401:
                        logger.error(f"Twelve Data ошибка: {data.get('message', 'Нет доступа')}")
                else:
                    logger.warning(f"Кукуруза API вернул статус {response.status}")
                    
        except Exception as e:
            logger.error(f"Corn API error: {e}")
        
        return self.last_successful_rates.get('CORN/USD', 4.50)
    
    async def fetch_from_fiat_api(self):
        """Получает курсы фиатных валют"""
        try:
            session = await self.get_session()
            url = "https://open.er-api.com/v6/latest/USD"
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    rates = data['rates']
                    
                    result = {}
                    if 'RUB' in rates:
                        result['USD/RUB'] = rates['RUB']
                    if 'EUR' in rates:
                        result['EUR/USD'] = 1.0 / rates['EUR']
                    if 'GBP' in rates:
                        result['GBP/USD'] = 1.0 / rates['GBP']
                    if 'JPY' in rates:
                        result['USD/JPY'] = rates['JPY']
                    
                    if 'EUR' in rates and 'GBP' in rates:
                        eur_usd = 1.0 / rates['EUR']
                        gbp_usd = 1.0 / rates['GBP']
                        result['EUR/GBP'] = eur_usd / gbp_usd
                    
                    return result
        except Exception as e:
            logger.error(f"Fiat API error: {e}")
            return {
                'EUR/USD': self.last_successful_rates.get('EUR/USD', 1.08),
                'GBP/USD': self.last_successful_rates.get('GBP/USD', 1.26),
                'USD/JPY': self.last_successful_rates.get('USD/JPY', 155.0),
                'USD/RUB': self.last_successful_rates.get('USD/RUB', 90.0),
                'EUR/GBP': self.last_successful_rates.get('EUR/GBP', 0.87)
            }
    
    async def fetch_rates(self):
        """Получает все курсы"""
        all_rates = {}
        
        # Фиатные валюты
        fiat = await self.fetch_from_fiat_api()
        if fiat:
            all_rates.update(fiat)
        
        # Криптовалюты
        crypto = await self.fetch_from_binance()
        if crypto:
            all_rates.update(crypto)
        
        # Металлы
        gold = await self.fetch_gold_price()
        all_rates['XAU/USD'] = gold
        
        silver = await self.fetch_silver_price()
        all_rates['XAG/USD'] = silver
        
        # Индексы
        indices = await self.fetch_indices()
        if indices:
            all_rates.update(indices)
        
        # Товары
        corn = await self.fetch_corn_price()
        all_rates['CORN/USD'] = corn
        
        if all_rates:
            self.last_successful_rates.update(all_rates)
            return all_rates
        
        return self.last_successful_rates
    
    async def send_telegram_message(self, chat_id, message):
        try:
            session = await self.get_session()
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            async with session.post(url, json=payload) as response:
                if response.status != 200:
                    logger.error(f"Telegram error: {await response.text()}")
        except Exception as e:
            logger.error(f"Error sending Telegram: {e}")
    
    async def send_telegram_message_with_keyboard(self, chat_id, message, keyboard):
        try:
            session = await self.get_session()
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            
            payload = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'reply_markup': json.dumps(keyboard)
            }
            async with session.post(url, json=payload) as response:
                if response.status != 200:
                    logger.error(f"Telegram error: {await response.text()}")
        except Exception as e:
            logger.error(f"Error sending keyboard: {e}")
    
    async def show_timezone_menu(self, chat_id):
        """Показывает меню выбора часового пояса"""
        keyboard = {"inline_keyboard": []}
        
        # Группируем пояса по 2 в ряд для компактности
        tz_list = list(TIMEZONES.items())
        for i in range(0, len(tz_list), 2):
            row = []
            for tz_key, tz_info in tz_list[i:i+2]:
                row.append({"text": tz_info['name'], "callback_data": f"tz_{tz_key}"})
            keyboard["inline_keyboard"].append(row)
        
        keyboard["inline_keyboard"].append([{"text": "◀️ Назад", "callback_data": "main_menu"}])
        
        await self.send_telegram_message_with_keyboard(
            chat_id,
            "🌍 <b>Выбери свой часовой пояс:</b>\n\n"
            "От этого зависит время в уведомлениях. Можно изменить в любой момент.",
            keyboard
        )
    
    async def set_user_timezone(self, chat_id, tz_key):
        """Устанавливает часовой пояс пользователя"""
        if tz_key in TIMEZONES:
            # Обновляем статистику с новым часовым поясом
            stats = load_user_stats()
            user_id = str(chat_id)
            if user_id in stats:
                stats[user_id]['timezone'] = tz_key
                stats[user_id]['timezone_name'] = TIMEZONES[tz_key]['name']
                save_user_stats(stats)
            
            await self.send_telegram_message(
                chat_id,
                f"✅ Часовой пояс установлен: {TIMEZONES[tz_key]['name']}\n\n"
                f"Теперь все уведомления будут приходить с твоим местным временем."
            )
            # После установки пояса возвращаем в главное меню
            await self.show_main_menu(chat_id)
        else:
            await self.send_telegram_message(chat_id, "❌ Ошибка: часовой пояс не найден")
            await self.show_main_menu(chat_id)
    
    async def show_stats(self, chat_id):
        """Показывает статистику использования бота (только для админа)"""
        if not self.is_admin(chat_id):
            await self.send_telegram_message(chat_id, "❌ У тебя нет доступа к статистике")
            await self.show_main_menu(chat_id)
            return
        
        stats = load_user_stats()
        
        if not stats:
            await self.send_telegram_message(chat_id, "📊 Статистика пока пуста")
            await self.show_main_menu(chat_id)
            return
        
        msg = "📊 <b>СТАТИСТИКА БОТА</b>\n\n"
        msg += f"👥 Всего пользователей: <b>{len(stats)}</b>\n"
        
        total_interactions = sum(u.get('interactions', 0) for u in stats.values())
        total_alerts = sum(u.get('alerts_created', 0) for u in stats.values())
        total_triggered = sum(u.get('alerts_triggered', 0) for u in stats.values())
        
        msg += f"💬 Всего сообщений: <b>{total_interactions}</b>\n"
        msg += f"🎯 Создано алертов: <b>{total_alerts}</b>\n"
        msg += f"⚡️ Сработало алертов: <b>{total_triggered}</b>\n\n"
        
        msg += "🏆 <b>Топ пользователей:</b>\n"
        top_users = sorted(stats.items(), key=lambda x: x[1].get('interactions', 0), reverse=True)[:5]
        
        for i, (user_id, data) in enumerate(top_users, 1):
            name = data.get('first_name', '')
            if data.get('username'):
                name += f" (@{data['username']})"
            msg += f"{i}. {name} — {data.get('interactions', 0)} сообщ.\n"
        
        msg += "\n📈 <b>Популярные пары:</b>\n"
        all_pairs = []
        for user_data in stats.values():
            all_pairs.extend(user_data.get('pairs', []))
        
        if all_pairs:
            pair_counts = Counter(all_pairs)
            for pair, count in pair_counts.most_common(5):
                msg += f"• {pair}: {count} раз(а)\n"
        
        await self.send_telegram_message(chat_id, msg)
        await self.show_main_menu(chat_id)
    
    async def handle_pair_management(self, chat_id, pair):
        """Показывает меню управления для конкретной пары"""
        user_id = str(chat_id)
        user_alerts_list = user_alerts.get(user_id, [])
        
        # Находим ВСЕ активные алерты для этой пары
        active_alerts = [alert for alert in user_alerts_list 
                         if alert.get('pair') == pair and alert.get('active')]
        
        if active_alerts:
            # Формируем список алертов
            alerts_text = ""
            for i, alert in enumerate(active_alerts, 1):
                alerts_text += f"{i}. 🎯 {alert['target']}\n"
            
            # Создаем клавиатуру с кнопками для каждого алерта
            keyboard = {"inline_keyboard": []}
            
            # Добавляем кнопку для каждого алерта
            for i, alert in enumerate(active_alerts, 1):
                keyboard["inline_keyboard"].append([
                    {"text": f"❌ Удалить алерт {i} ({alert['target']})", 
                     "callback_data": f"delete_specific_{pair}_{i}"}
                ])
            
            # Кнопка для добавления новой цели
            keyboard["inline_keyboard"].append([
                {"text": "➕ Добавить цель", "callback_data": f"add_{pair}"}
            ])
            
            # Кнопка назад
            keyboard["inline_keyboard"].append([
                {"text": "◀️ Назад", "callback_data": "main_menu"}
            ])
            
            await self.send_telegram_message_with_keyboard(
                chat_id,
                f"📊 {pair}\n\n"
                f"Всего алертов: {len(active_alerts)}\n\n"
                f"{alerts_text}",
                keyboard
            )
        else:
            # Нет алертов - запускаем создание
            self.alert_states[str(chat_id)] = {'pair': pair, 'step': 'waiting_price'}
            
            cancel_keyboard = {
                "inline_keyboard": [
                    [{"text": "◀️ Отмена", "callback_data": "main_menu"}]
                ]
            }
            
            await self.send_telegram_message_with_keyboard(
                chat_id,
                f"Создать алерт для {pair}\n\n"
                f"📝 Введи целевую цену:",
                cancel_keyboard
            )
    
    async def show_main_menu(self, chat_id):
        """Главное меню с одной колонкой"""
        rates = await self.fetch_rates()
        if not rates:
            # Если не удалось получить курсы, показываем упрощенное меню
            keyboard = {
                "inline_keyboard": [
                    [{"text": "📩 Обратная связь", "callback_data": "collaboration"}],
                    [{"text": "🌍 Часовой пояс", "callback_data": "show_timezone"}]
                ]
            }
            await self.send_telegram_message_with_keyboard(chat_id, "🔍 Выбери действие:", keyboard)
            return
        
        # Получаем алерты пользователя
        user_id = str(chat_id)
        user_alerts_list = user_alerts.get(user_id, [])
        
        # Функция для получения индикатора количества алертов
        def get_alert_indicator(count):
            if count == 0:
                return ""
            elif count == 1:
                return " 1️⃣"
            elif count == 2:
                return " 2️⃣"
            elif count == 3:
                return " 3️⃣"
            elif count == 4:
                return " 4️⃣"
            elif count == 5:
                return " 5️⃣"
            else:
                return f" {count}️⃣"
        
        # Собираем все доступные пары с их данными
        all_pairs = []
        
        # Валюты
        currency_pairs = ['EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/RUB', 'EUR/GBP']
        for pair in currency_pairs:
            if pair in rates:
                rate = rates[pair]
                alert_count = sum(1 for alert in user_alerts_list 
                                  if alert.get('pair') == pair and alert.get('active'))
                indicator = get_alert_indicator(alert_count)
                text = f"💶 {pair}: {rate:.4f}{indicator}"
                all_pairs.append({
                    'pair': pair,
                    'text': text
                })
        
        # Металлы
        metals = ['XAU/USD', 'XAG/USD']
        for pair in metals:
            if pair in rates:
                rate = rates[pair]
                alert_count = sum(1 for alert in user_alerts_list 
                                  if alert.get('pair') == pair and alert.get('active'))
                indicator = get_alert_indicator(alert_count)
                text = f"🏅 {pair}: ${rate:,.2f}{indicator}"
                all_pairs.append({
                    'pair': pair,
                    'text': text
                })
        
        # Крипта
        crypto_pairs = ['BTC/USD', 'ETH/USD', 'SOL/USD', 'BNB/USD', 'LINK/USD', 'TON/USD', 'XRP/USD', 'DOGE/USD', 'AVAX/USD']
        for pair in crypto_pairs:
            if pair in rates:
                rate = rates[pair]
                alert_count = sum(1 for alert in user_alerts_list 
                                  if alert.get('pair') == pair and alert.get('active'))
                indicator = get_alert_indicator(alert_count)
                
                if pair in ['BTC/USD', 'ETH/USD']:
                    text = f"₿ {pair}: ${rate:,.2f}{indicator}"
                elif pair in ['SOL/USD', 'BNB/USD', 'AVAX/USD', 'LINK/USD']:
                    text = f"🟪 {pair}: ${rate:.2f}{indicator}"
                elif pair in ['XRP/USD', 'DOGE/USD', 'TON/USD']:
                    text = f"⚡️ {pair}: ${rate:.4f}{indicator}"
                else:
                    text = f"🪙 {pair}: ${rate:.2f}{indicator}"
                
                all_pairs.append({
                    'pair': pair,
                    'text': text
                })
        
        # Индексы
        indices = ['S&P 500', 'NASDAQ']
        for pair in indices:
            if pair in rates:
                rate = rates[pair]
                alert_count = sum(1 for alert in user_alerts_list 
                                  if alert.get('pair') == pair and alert.get('active'))
                indicator = get_alert_indicator(alert_count)
                text = f"📈 {pair}: ${rate:,.2f}{indicator}"
                all_pairs.append({
                    'pair': pair,
                    'text': text
                })
        
        # Товары
        if 'CORN/USD' in rates:
            rate = rates['CORN/USD']
            alert_count = sum(1 for alert in user_alerts_list 
                              if alert.get('pair') == 'CORN/USD' and alert.get('active'))
            indicator = get_alert_indicator(alert_count)
            text = f"🌽 CORN/USD: ${rate:.2f}{indicator}"
            all_pairs.append({
                'pair': 'CORN/USD',
                'text': text
            })
        
        # Сортируем все пары по алфавиту
        all_pairs.sort(key=lambda x: x['pair'])
        
        # Формируем одноколоночную клавиатуру
        keyboard = {"inline_keyboard": []}
        
        for item in all_pairs:
            pair = item['pair']
            text = item['text']
            keyboard["inline_keyboard"].append([
                {"text": text, "callback_data": f"manage_{pair}"}
            ])
        
        # Кнопки внизу
        keyboard["inline_keyboard"].append([
            {"text": "📩 Обратная связь", "callback_data": "collaboration"},
            {"text": "🌍 Часовой пояс", "callback_data": "show_timezone"}
        ])
        
        await self.send_telegram_message_with_keyboard(chat_id, "📊 Нажми на пару для управления:", keyboard)
    
    async def handle_alert_input(self, chat_id, text):
        try:
            text = text.replace(',', '.')
            target = float(text)
            
            if str(chat_id) not in self.alert_states:
                await self.send_telegram_message(chat_id, "❌ Ошибка: начни сначала /start")
                await self.show_main_menu(chat_id)
                return
                
            state = self.alert_states[str(chat_id)]
            if 'pair' not in state:
                await self.send_telegram_message(chat_id, "❌ Ошибка: выбери пару сначала")
                await self.show_main_menu(chat_id)
                return
                
            pair = state['pair']
            
            user_id = str(chat_id)
            if user_id not in user_alerts:
                user_alerts[user_id] = []
            
            alert = {
                'pair': pair,
                'target': target,
                'active': True
            }
            
            user_alerts[user_id].append(alert)
            save_user_alerts(user_alerts)
            
            stats = load_user_stats()
            if user_id in stats:
                stats[user_id]['alerts_created'] = stats[user_id].get('alerts_created', 0) + 1
                stats[user_id]['pairs'] = stats[user_id].get('pairs', []) + [pair]
                save_user_stats(stats)
            
            del self.alert_states[str(chat_id)]
            
            # Подтверждение создания алерта
            await self.send_telegram_message(
                chat_id,
                f"✅ Алерт для {pair} создан!\n\n"
                f"🎯 Цель: {target}"
            )
            
            # Возвращаем в главное меню
            await self.show_main_menu(chat_id)
            
        except ValueError:
            await self.send_telegram_message(chat_id, "❌ Это не число! Введи цену (например: 1.10)")
            # Оставляем состояние активным, даём ещё попытку
        except Exception as e:
            logger.error(f"Error in alert input: {e}")
            await self.send_telegram_message(chat_id, "❌ Ошибка при создании алерта")
            await self.show_main_menu(chat_id)
    
    async def list_alerts(self, chat_id):
        user_id = str(chat_id)
        alerts = user_alerts.get(user_id, [])
        
        if not alerts:
            await self.send_telegram_message(chat_id, "📭 У тебя пока нет алертов")
            return
        
        keyboard = {"inline_keyboard": []}
        msg = "📋 Твои алерты:\n\n"
        
        for i, alert in enumerate(alerts, 1):
            status = "✅" if alert.get('active', False) else "⚡️"
            target = alert.get('target') or alert.get('target_price') or '?'
            pair = alert.get('pair', '?')
            msg += f"{i}. {status} {pair} = {target}\n"
            keyboard["inline_keyboard"].append(
                [{"text": f"❌ Удалить {i}", "callback_data": f"delete_{i}"}]
            )
        
        keyboard["inline_keyboard"].append([{"text": "◀️ Назад", "callback_data": "main_menu"}])
        await self.send_telegram_message_with_keyboard(chat_id, msg, keyboard)
    
    async def handle_telegram_commands(self, update):
        try:
            if 'message' not in update:
                return
            
            msg = update['message']
            chat_id = msg['chat']['id']
            text = msg.get('text', '')
            
            username = msg['chat'].get('username', '')
            first_name = msg['chat'].get('first_name', '')
            last_name = msg['chat'].get('last_name', '')
            
            # Обновляем статистику
            update_user_stats(chat_id, username, first_name, last_name)
            
            if not self.is_user_allowed(chat_id):
                logger.info(f"⛔ Запрещен: {chat_id}")
                return
            
            if text in ['/start', '/menu']:
                if str(chat_id) in self.alert_states:
                    del self.alert_states[str(chat_id)]
                await self.show_main_menu(chat_id)
                return
            
            if text == '/stats':
                await self.show_stats(chat_id)
                return
            
            if text == '/timezone':
                await self.show_timezone_menu(chat_id)
                return
            
            if str(chat_id) in self.alert_states:
                await self.handle_alert_input(chat_id, text)
                return
            
            if text == '/alert':
                await self.handle_pair_management(chat_id, 'EUR/USD')
            else:
                await self.show_main_menu(chat_id)
                
        except Exception as e:
            logger.error(f"Error in handle_telegram_commands: {e}")
    
    async def handle_callback_query(self, update):
        try:
            if 'callback_query' not in update:
                return
            
            cb = update['callback_query']
            chat_id = cb['message']['chat']['id']
            data = cb['data']
            
            username = cb['from'].get('username', '')
            first_name = cb['from'].get('first_name', '')
            last_name = cb['from'].get('last_name', '')
            update_user_stats(chat_id, username, first_name, last_name)
            
            if not self.is_user_allowed(chat_id):
                return
            
            session = await self.get_session()
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
            await session.post(url, json={'callback_query_id': cb['id']})
            
            if data == "main_menu":
                if str(chat_id) in self.alert_states:
                    del self.alert_states[str(chat_id)]
                await self.show_main_menu(chat_id)
            elif data == "show_timezone":
                await self.show_timezone_menu(chat_id)
            elif data.startswith("tz_"):
                tz_key = data.replace("tz_", "")
                await self.set_user_timezone(chat_id, tz_key)
            elif data.startswith("manage_"):
                pair = data.replace("manage_", "")
                await self.handle_pair_management(chat_id, pair)
            elif data.startswith("delete_specific_"):
                # Формат: delete_specific_EUR/USD_1
                try:
                    # Разбираем строку
                    parts = data.replace("delete_specific_", "").rsplit("_", 1)
                    pair = parts[0]
                    alert_num = int(parts[1]) - 1
                    
                    user_id = str(chat_id)
                    if user_id in user_alerts:
                        # Находим все алерты для этой пары
                        pair_alerts = [alert for alert in user_alerts[user_id] 
                                       if alert.get('pair') == pair and alert.get('active')]
                        
                        if 0 <= alert_num < len(pair_alerts):
                            # Находим конкретный алерт в общем списке
                            target_alert = pair_alerts[alert_num]
                            # Удаляем его
                            user_alerts[user_id] = [a for a in user_alerts[user_id] 
                                                     if not (a.get('pair') == pair and 
                                                            a.get('target') == target_alert['target'] and 
                                                            a.get('active'))]
                            save_user_alerts(user_alerts)
                            
                            # Проверяем, остались ли еще алерты для этой пары
                            remaining_alerts = [a for a in user_alerts[user_id] 
                                               if a.get('pair') == pair and a.get('active')]
                            
                            if not remaining_alerts:
                                # Если алертов не осталось, показываем сообщение и открываем меню создания
                                await self.send_telegram_message(chat_id, f"✅ Все алерты для {pair} удалены")
                                await self.handle_pair_management(chat_id, pair)
                                return
                except Exception as e:
                    logger.error(f"Delete specific error: {e}")
                
                # Если остались алерты, возвращаемся к управлению
                await self.handle_pair_management(chat_id, pair)
            elif data.startswith("delete_all_"):
                pair = data.replace("delete_all_", "")
                user_id = str(chat_id)
                if user_id in user_alerts:
                    # Удаляем все алерты для этой пары
                    old_count = len([a for a in user_alerts[user_id] 
                                     if a.get('pair') == pair and a.get('active')])
                    user_alerts[user_id] = [a for a in user_alerts[user_id] 
                                             if not (a.get('pair') == pair and a.get('active'))]
                    save_user_alerts(user_alerts)
                    logger.info(f"Удалено {old_count} алертов для {pair} у пользователя {user_id}")
                    
                    # Показываем сообщение и открываем меню создания
                    await self.send_telegram_message(chat_id, f"✅ Все алерты для {pair} удалены")
                    await self.handle_pair_management(chat_id, pair)
                    return
            elif data.startswith("add_"):
                pair = data.replace("add_", "")
                # Запускаем процесс добавления новой цели
                self.alert_states[str(chat_id)] = {'pair': pair, 'step': 'waiting_price'}
                cancel_keyboard = {
                    "inline_keyboard": [
                        [{"text": "◀️ Отмена", "callback_data": f"manage_{pair}"}]
                    ]
                }
                await self.send_telegram_message_with_keyboard(
                    chat_id,
                    f"Создать алерт для {pair}\n\n"
                    f"📝 Введи целевую цену:",
                    cancel_keyboard
                )
            elif data == "collaboration":
                collab_text = (
                    "📩 <b>Обратная связь</b>\n\n"
                    "📈 Нет какой-то валютной пары в списке?\n"
                    "✉️ Напиши @Maranafa2023 — добавим!\n\n"
                    "Спасибо, что пользуетесь ботом! 🚀"
                )
                await self.send_telegram_message(chat_id, collab_text)
            elif data == "cancel_alert":
                if str(chat_id) in self.alert_states:
                    del self.alert_states[str(chat_id)]
                await self.send_telegram_message(chat_id, "❌ Создание отменено")
                await self.show_main_menu(chat_id)
            elif data.startswith("delete_"):
                # Старый формат удаления - для обратной совместимости
                try:
                    num = int(data.replace("delete_", "")) - 1
                    user_id = str(chat_id)
                    if user_id in user_alerts and 0 <= num < len(user_alerts[user_id]):
                        user_alerts[user_id].pop(num)
                        save_user_alerts(user_alerts)
                        await self.send_telegram_message(chat_id, f"✅ Алерт {num+1} удален")
                        await self.list_alerts(chat_id)
                except Exception as e:
                    logger.error(f"Delete error: {e}")
                    await self.show_main_menu(chat_id)
                    
        except Exception as e:
            logger.error(f"Callback error: {e}")
            # В случае ошибки возвращаем в главное меню
            await self.show_main_menu(chat_id)
    
    async def get_updates(self):
        try:
            session = await self.get_session()
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            
            if self.last_update_id > 0:
                url += f"?offset={self.last_update_id + 1}"
            
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    for update in data.get('result', []):
                        await self.handle_telegram_commands(update)
                        await self.handle_callback_query(update)
                        if update['update_id'] > self.last_update_id:
                            self.last_update_id = update['update_id']
        except Exception as e:
            logger.error(f"Updates error: {e}")
    
    async def check_thresholds(self, rates):
        """Проверяет достижение целей"""
        notifications = []
        stats = load_user_stats()
        now_utc = datetime.now(ZoneInfo('UTC'))
        
        for user_id, alerts in user_alerts.items():
            # Получаем часовой пояс пользователя (по умолчанию Москва)
            user_tz = stats.get(str(user_id), {}).get('timezone', 'Europe/Moscow')
            tz_info = TIMEZONES.get(user_tz, TIMEZONES['Europe/Moscow'])
            user_time = now_utc.astimezone(ZoneInfo(user_tz))
            current_time = user_time.strftime('%H:%M:%S')
            
            for alert in alerts:
                if not alert.get('active', False):
                    continue
                
                target = alert.get('target') or alert.get('target_price')
                if target is None:
                    continue
                    
                pair = alert.get('pair')
                if not pair or pair not in rates:
                    continue
                
                current = rates[pair]
                
                if pair in ['BTC/USD', 'ETH/USD', 'XAU/USD', 'S&P 500', 'NASDAQ']:
                    if abs(current - target) / target < 0.0001:
                        msg = (
                            f"🎯 <b>ЦЕЛЬ ДОСТИГНУТА!</b>\n\n"
                            f"📊 {pair}\n"
                            f"🎯 Цель: {target:.2f}\n"
                            f"⏱️ {current_time} ({tz_info['name']})"
                        )
                        notifications.append((int(user_id), msg))
                        alert['active'] = False
                        
                        if user_id in stats:
                            stats[user_id]['alerts_triggered'] = stats[user_id].get('alerts_triggered', 0) + 1
                        
                        save_user_alerts(user_alerts)
                        logger.info(f"Цель {pair}: {current:.2f}")
                
                elif pair in ['DOGE/USD', 'XRP/USD', 'TON/USD']:
                    if abs(current - target) <= 0.0001:
                        msg = (
                            f"🎯 <b>ЦЕЛЬ ДОСТИГНУТА!</b>\n\n"
                            f"📊 {pair}\n"
                            f"🎯 Цель: {target:.4f}\n"
                            f"⏱️ {current_time} ({tz_info['name']})"
                        )
                        notifications.append((int(user_id), msg))
                        alert['active'] = False
                        
                        if user_id in stats:
                            stats[user_id]['alerts_triggered'] = stats[user_id].get('alerts_triggered', 0) + 1
                        
                        save_user_alerts(user_alerts)
                        logger.info(f"Цель {pair}: {current:.4f}")
                
                else:
                    if abs(current - target) <= 0.00005:
                        msg = (
                            f"🎯 <b>ЦЕЛЬ ДОСТИГНУТА!</b>\n\n"
                            f"📊 {pair}\n"
                            f"🎯 Цель: {target:.5f}\n"
                            f"⏱️ {current_time} ({tz_info['name']})"
                        )
                        notifications.append((int(user_id), msg))
                        alert['active'] = False
                        
                        if user_id in stats:
                            stats[user_id]['alerts_triggered'] = stats[user_id].get('alerts_triggered', 0) + 1
                        
                        save_user_alerts(user_alerts)
                        logger.info(f"Цель {pair}: {current:.5f}")
        
        save_user_stats(stats)
        return notifications
    
    async def check_rates_task(self, interval=10):
        while True:
            try:
                rates = await self.fetch_rates()
                if rates:
                    notifications = await self.check_thresholds(rates)
                    for chat_id, msg in notifications:
                        if self.is_user_allowed(chat_id):
                            await self.send_telegram_message(chat_id, msg)
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Rates task error: {e}")
                await asyncio.sleep(interval)
    
    async def check_commands_task(self, interval=2):
        while True:
            try:
                await self.get_updates()
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Commands task error: {e}")
                await asyncio.sleep(5)
    
    async def health_check(self, request):
        return web.Response(text="OK")
    
    async def self_ping_task(self):
        while True:
            try:
                await asyncio.sleep(240)
                
                render_url = os.environ.get('RENDER_EXTERNAL_URL')
                if not render_url:
                    render_url = "http://localhost:8080"
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{render_url}/health", timeout=30) as response:
                        if response.status == 200:
                            logger.info("✅ Самопинг успешен")
                        else:
                            logger.warning(f"⚠️ Самопинг вернул {response.status}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Ошибка самопинга: {e}")
                continue
    
    async def run(self):
        mode = "ОТКРЫТЫЙ" if not PRIVATE_MODE else "ПРИВАТНЫЙ"
        logger.info(f"🚀 ЗАПУСК БОТА [{mode} РЕЖИМ]")
        logger.info(f"⚡️ Проверка: каждые 10 секунд")
        logger.info(f"📊 Пары: фиат + металлы + крипта + индексы + товары")
        logger.info(f"🎯 Точность: максимальная")
        logger.info(f"🌍 Поддержка часовых поясов: {len(TIMEZONES)} городов")
        
        app = web.Application()
        app.router.add_get('/health', self.health_check)
        
        port = int(os.environ.get('PORT', 8080))
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        logger.info(f"🌐 Веб-сервер для пинга запущен на порту {port}")
        
        try:
            await asyncio.gather(
                self.check_rates_task(interval=10),
                self.check_commands_task(interval=2),
                self.self_ping_task()
            )
        except KeyboardInterrupt:
            logger.info("⏹ Остановлено")
        finally:
            await runner.cleanup()
            if self.session:
                await self.session.close()

async def main():
    monitor = CurrencyMonitor()
    await monitor.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Программа завершена")
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
import os
import json
import sys
import re
import random
from dotenv import load_dotenv
from aiohttp import web
from zoneinfo import ZoneInfo
from collections import Counter

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("🚀 Бот запускается...")

# Проверяем наличие yfinance
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
    logger.info("✅ yfinance доступен")
except ImportError:
    YFINANCE_AVAILABLE = False
    logger.warning("⚠️ yfinance не установлен, индексы будут через другие источники")

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

# Словарь для конвертации цифр в эмодзи
DIGIT_TO_EMOJI = {
    '0': '0️⃣',
    '1': '1️⃣',
    '2': '2️⃣',
    '3': '3️⃣',
    '4': '4️⃣',
    '5': '5️⃣',
    '6': '6️⃣',
    '7': '7️⃣',
    '8': '8️⃣',
    '9': '9️⃣',
}

def number_to_emoji(num):
    """Конвертирует число в эмодзи-цифры (например, 10 -> 1️⃣0️⃣)"""
    return ''.join(DIGIT_TO_EMOJI[d] for d in str(num))

def get_seasonal_slogan():
    """Возвращает слоган с учётом времени года"""
    now = datetime.now()
    current_month = now.month
    
    # Сезонные слоганы
    winter_slogans = [
        "❄️ Зимние ставки",
        "🎄 Рынок под ёлкой",
        "☃️ Снежный тренд",
        "🧊 Холодный расчёт",
        "⛷️ Вниз как на лыжах",
        "🎿 Зимнее ралли",
        "❄️ Кристально чистый анализ",
    ]
    
    spring_slogans = [
        "🌸 Весеннее ралли",
        "🌷 Цены цветут",
        "🌱 Бычий рынок просыпается",
        "☔️ Дождливая коррекция",
        "🐣 Птенцы прибыли",
        "🌼 Апрельский рост",
        "🍀 Удача зелёная",
    ]
    
    summer_slogans = [
        "☀️ Летний зной на рынке",
        "🍦 Сладкие профиты",
        "🏖️ Пляжный трейдинг",
        "🌊 Волна доходности",
        "🍉 Сочные цены",
        "⚡️ Грозовой скачок",
        "🌴 Карибское ралли",
    ]
    
    autumn_slogans = [
        "🍂 Осенняя волатильность",
        "🍁 Листопад доходности",
        "🌧️ Дождливая коррекция",
        "🎃 Хэллоуинский скачок",
        "🍄 Грибной сезон",
        "🌬️ Ветер перемен",
        "🏮 Ноябрьский рост",
    ]
    
    # Универсальные слоганы
    universal_slogans = [
        "💰 Цена имеет значение",
        "🎯 Поймай момент",
        "⚡️ Быстрее рынка",
        "📈 Твой личный скальпер",
        "🔥 Где деньги? Здесь.",
        "🚀 Ловим луну вместе",
        "💸 Деньги любят счёт",
        "🎰 Играй по-крупному",
        "💹 Мониторинг 24/7",
        "🤑 Ни одной упущенной цели",
        "😎 Бот не спит — ты отдыхаешь",
        "🎯 Точность — вежливость королей",
        "📊 Цены в реальном времени",
        "🪙 Крипта, валюта, металлы",
        "💎 Твой финансовый помощник",
        "📈 Следи за ценой — зарабатывай",
        "🐂 Время покупать",
        "🐻 Осторожно, коррекция",
        "🍺 Пятница, цены падают",
        "🎉 Выходные близко",
    ]
    
    # Мудрые цитаты
    wise_slogans = [
        "🧠 Рынок переводит деньги от нетерпеливых к терпеливым — У. Баффет",
        "🎯 Важно не быть правым, а сколько ты зарабатываешь когда прав — Дж. Сорос",
        "🛡️ Лучшие трейдеры не самые умные, а самые дисциплинированные",
        "⏳ Деньги делают, выжидая, а не торгуя — Д. Ливермор",
        "📉 Позволять убыткам расти — самая серьезная ошибка — У. О'Нил",
        "🧘 Учитесь принимать убытки — М. Шварц",
        "🌊 Рынок — океан, волны эмоций бьются о скалы дисциплины",
        "📊 Свечи на графике — истории жадности и страха",
        "💃 Трейдинг — танец с хаосом",
        "🎭 Рынок никогда не бывает очевиден — Д. Ливермор",
        "🧪 Если вы не контролируете эмоции, вы не контролируете деньги — У. Баффет",
        "🏦 Фондовый рынок заполнен людьми, знающими цену всему, но не ценность — Ф. Фишер",
        "⏰ Время — друг, импульс — враг — Д. Богл",
        "🔮 Лучшая формация — та, которую ты не торгуешь",
        "⚖️ Риск возникает из незнания того, что вы делаете — У. Баффет",
        "🎲 Торговля — игра вероятностей",
        "📝 Планируй торговлю и торгуй по плану — М. Дуглас",
        "🧗 Выживание — единственный путь к богатству — П. Бернстайн",
        "🐑 Люди сходят с ума толпой, а приходят в себя поодиночке — Ч. Маккей",
        "⚠️ «На этот раз всё по-другому» — самые опасные слова — Д. Темплтон",
        "📈 Бычий рынок рождается на пессимизме, умирает на эйфории — Д. Темплтон",
        "🤔 Если не знаешь себя, рынок — дорогое место чтобы это выяснить — А. Смит",
        "💰 Бойся, когда другие жадны. Будь жадным, когда другие боятся — У. Баффет",
        "🧠 Трейдинг раскрывает характер и формирует его — И. Бьеджи",
        "🎪 Рынок одурачивает как можно больше людей — Б. Барух",
        "📚 Учитесь на чужих ошибках — на своих учиться слишком долго",
        "🔁 Что было, то будет — рынки повторяются — Д. Ливермор",
        "🧘‍♂️ Не пытайся вести рынок — учись чувствовать его импульс",
        "📉 Прибыль — это правильное действие в правильном месте — Дж. Сорос",
        "🧠 Интуиция трейдера — это сжатый опыт",
        "💪 Сила воли — капитал, самодисциплина — процентная ставка",
        "🎯 Вход — искусство, выход — талант",
        "📊 Рынок — тест на эмоциональную зрелость",
        "🧘 Терпение — не просто ожидание, а сохранение фокуса",
        "📈 Тренд — твой друг до последнего",
        "🛑 Стоп-лосс — ремень безопасности трейдера",
        "🚀 Удача улыбается тем, кто готов к ее улыбке",
    ]
    
    # Выбираем сезонные слоганы в зависимости от месяца
    if current_month == 12 or current_month <= 2:  # Зима (дек, янв, фев)
        seasonal = winter_slogans
    elif 3 <= current_month <= 5:  # Весна (март, апр, май)
        seasonal = spring_slogans
    elif 6 <= current_month <= 8:  # Лето (июнь, июль, авг)
        seasonal = summer_slogans
    else:  # Осень (сент, окт, нояб)
        seasonal = autumn_slogans
    
    # Объединяем: 30% сезонные, 40% универсальные, 30% мудрые
    all_slogans = seasonal * 3 + universal_slogans * 4 + wise_slogans * 3
    
    return random.choice(all_slogans)

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

def update_user_stats(chat_id, username, first_name, last_name, pair=None, timezone=None, slogan=None, slogan_time=None, pinned_pairs=None):
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
            'timezone': 'Europe/Moscow',
            'timezone_name': 'Москва (UTC+3)',
            'current_slogan': get_seasonal_slogan(),
            'slogan_updated': datetime.now().isoformat(),
            'pinned_pairs': []
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
    
    if slogan:
        stats[user_id]['current_slogan'] = slogan
    
    if slogan_time:
        stats[user_id]['slogan_updated'] = slogan_time.isoformat()
    
    if pinned_pairs is not None:
        stats[user_id]['pinned_pairs'] = pinned_pairs
    
    save_user_stats(stats)
    return stats[user_id]

def get_user_timezone(user_id):
    """Возвращает часовой пояс пользователя"""
    stats = load_user_stats()
    user_id = str(user_id)
    if user_id in stats and 'timezone' in stats[user_id]:
        return stats[user_id]['timezone']
    return 'Europe/Moscow'

def get_user_slogan(user_id):
    """Возвращает слоган для пользователя с учётом времени года (обновляется раз в 24 часа)"""
    stats = load_user_stats()
    user_id = str(user_id)
    now = datetime.now()
    
    # Если пользователь есть в статистике
    if user_id in stats:
        current_slogan = stats[user_id].get('current_slogan')
        slogan_updated = stats[user_id].get('slogan_updated')
        
        if slogan_updated:
            last_update = datetime.fromisoformat(slogan_updated)
            hours_passed = (now - last_update).total_seconds() / 3600
            
            # Если прошло меньше 24 часов, возвращаем текущий слоган
            if hours_passed < 24:
                return current_slogan
    
    # Выбираем новый сезонный слоган
    new_slogan = get_seasonal_slogan()
    
    # Если есть текущий слоган и он совпадает с новым, пробуем еще раз
    if user_id in stats and 'current_slogan' in stats[user_id]:
        attempts = 0
        while new_slogan == stats[user_id]['current_slogan'] and attempts < 10:
            new_slogan = get_seasonal_slogan()
            attempts += 1
    
    # Обновляем статистику
    if user_id in stats:
        stats[user_id]['current_slogan'] = new_slogan
        stats[user_id]['slogan_updated'] = now.isoformat()
        save_user_stats(stats)
    else:
        # Если пользователя нет в статистике
        update_user_stats(int(user_id), '', '', '', slogan=new_slogan, slogan_time=now)
    
    return new_slogan

def get_user_pinned_pairs(user_id):
    """Возвращает закрепленные пары пользователя"""
    stats = load_user_stats()
    user_id = str(user_id)
    if user_id in stats and 'pinned_pairs' in stats[user_id]:
        return stats[user_id]['pinned_pairs']
    return []

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
            # Валюты (9 пар)
            'EUR/USD': 1.08,
            'GBP/USD': 1.26,
            'USD/JPY': 155.0,
            'USD/RUB': 90.0,
            'EUR/GBP': 0.87,
            'USD/CAD': 1.35,
            'AUD/USD': 0.65,
            'USD/CHF': 0.88,
            'USD/CNY': 7.25,
            
            # Металлы (3 пары)
            'XAU/USD': 5160.0,
            'XAG/USD': 30.0,
            'XPT/USD': 1000.0,
            
            # Крипта (5 пар)
            'BTC/USD': 67000.0,
            'ETH/USD': 1950.0,
            'SOL/USD': 84.0,
            'XRP/USD': 1.40,
            'DOGE/USD': 0.098,
            
            # Индексы (2 пары)
            'S&P 500': 5100.0,
            'NASDAQ': 18000.0,
            
            # Товары (3 пары)
            'CORN/USD': 4.50,
            'WTI/USD': 75.0,
            'BRENT/USD': 78.0,
        }
        
        # Для кэширования индексов
        self.last_indices_update = None
        self.cached_indices = None
        
        # Списки для форматирования цен
        self.currency_pairs = ['EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/RUB', 'EUR/GBP', 'USD/CAD', 'AUD/USD', 'USD/CHF', 'USD/CNY']
        self.high_value_pairs = ['BTC/USD', 'ETH/USD', 'XAU/USD', 'XPT/USD', 'S&P 500', 'NASDAQ']
        self.low_value_pairs = ['DOGE/USD', 'XRP/USD']
    
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
        """Получает курсы криптовалют с Binance (только выбранные)"""
        try:
            session = await self.get_session()
            result = {}
            
            symbols = {
                'BTC': 'BTCUSDT',
                'ETH': 'ETHUSDT',
                'SOL': 'SOLUSDT',
                'XRP': 'XRPUSDT',
                'DOGE': 'DOGEUSDT',
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
    
    async def fetch_platinum_price(self):
        """Получает цену платины через Gold-API"""
        try:
            session = await self.get_session()
            url = "https://api.gold-api.com/price/XPT"
            
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    price = float(data['price'])
                    
                    if price and price > 500 and price < 5000:
                        logger.info(f"✅ Платина: ${price:.2f}/унция")
                        return price
        except Exception as e:
            logger.error(f"Platinum API error: {e}")
        
        return self.last_successful_rates.get('XPT/USD', 1000.0)
    
    async def fetch_oil_prices(self):
        """Получает цены на нефть через yfinance"""
        if YFINANCE_AVAILABLE:
            try:
                wti = yf.Ticker("CL=F")
                brent = yf.Ticker("BZ=F")
                
                wti_info = wti.info
                brent_info = brent.info
                
                result = {}
                
                if 'regularMarketPrice' in wti_info:
                    result['WTI/USD'] = float(wti_info['regularMarketPrice'])
                elif 'currentPrice' in wti_info:
                    result['WTI/USD'] = float(wti_info['currentPrice'])
                
                if 'regularMarketPrice' in brent_info:
                    result['BRENT/USD'] = float(brent_info['regularMarketPrice'])
                elif 'currentPrice' in brent_info:
                    result['BRENT/USD'] = float(brent_info['currentPrice'])
                
                if result:
                    logger.info(f"✅ Нефть: WTI ${result.get('WTI/USD', 0):.2f}, BRENT ${result.get('BRENT/USD', 0):.2f}")
                    return result
            except Exception as e:
                logger.warning(f"Oil price error: {e}")
        
        return {
            'WTI/USD': self.last_successful_rates.get('WTI/USD', 75.0),
            'BRENT/USD': self.last_successful_rates.get('BRENT/USD', 78.0)
        }
    
    async def fetch_indices(self):
        """Получает значения индексов из нескольких источников с переключением"""
        now = datetime.now()
        result = {}
        
        # Проверяем кэш (обновляем не чаще раза в минуту)
        if self.last_indices_update and self.cached_indices:
            if (now - self.last_indices_update).total_seconds() < 60:
                logger.info("📊 Индексы из кэша (обновление раз в минуту)")
                return self.cached_indices
        
        # Источник 1: yfinance (если доступен)
        if YFINANCE_AVAILABLE:
            try:
                spy = yf.Ticker("SPY")
                qqq = yf.Ticker("QQQ")
                
                spy_info = spy.info
                qqq_info = qqq.info
                
                if 'regularMarketPrice' in spy_info:
                    result['S&P 500'] = float(spy_info['regularMarketPrice'])
                elif 'currentPrice' in spy_info:
                    result['S&P 500'] = float(spy_info['currentPrice'])
                
                if 'regularMarketPrice' in qqq_info:
                    result['NASDAQ'] = float(qqq_info['regularMarketPrice'])
                elif 'currentPrice' in qqq_info:
                    result['NASDAQ'] = float(qqq_info['currentPrice'])
                
                if result:
                    logger.info("✅ Индексы от yfinance")
                    self.cached_indices = result
                    self.last_indices_update = now
                    return result
            except Exception as e:
                logger.warning(f"yfinance error: {e}")
        
        # Если все источники упали, возвращаем кэш
        logger.warning("⚠️ Все источники индексов недоступны, использую кэш")
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
        """Получает курсы фиатных валют (все 9 пар)"""
        try:
            session = await self.get_session()
            url = "https://open.er-api.com/v6/latest/USD"
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    rates = data['rates']
                    
                    result = {}
                    
                    # Основные валюты
                    if 'RUB' in rates:
                        result['USD/RUB'] = rates['RUB']
                    if 'EUR' in rates:
                        result['EUR/USD'] = 1.0 / rates['EUR']
                    if 'GBP' in rates:
                        result['GBP/USD'] = 1.0 / rates['GBP']
                    if 'JPY' in rates:
                        result['USD/JPY'] = rates['JPY']
                    if 'CNY' in rates:
                        result['USD/CNY'] = 1.0 / rates['CNY']
                    if 'CAD' in rates:
                        result['USD/CAD'] = rates['CAD']
                    if 'AUD' in rates:
                        result['AUD/USD'] = 1.0 / rates['AUD']
                    if 'CHF' in rates:
                        result['USD/CHF'] = rates['CHF']
                    
                    # EUR/GBP
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
                'EUR/GBP': self.last_successful_rates.get('EUR/GBP', 0.87),
                'USD/CAD': self.last_successful_rates.get('USD/CAD', 1.35),
                'AUD/USD': self.last_successful_rates.get('AUD/USD', 0.65),
                'USD/CHF': self.last_successful_rates.get('USD/CHF', 0.88),
                'USD/CNY': self.last_successful_rates.get('USD/CNY', 7.25),
            }
    
    async def fetch_rates(self):
        """Получает все курсы"""
        all_rates = {}
        
        # Фиатные валюты (9 пар)
        fiat = await self.fetch_from_fiat_api()
        if fiat:
            all_rates.update(fiat)
        
        # Криптовалюты (5 пар)
        crypto = await self.fetch_from_binance()
        if crypto:
            all_rates.update(crypto)
        
        # Металлы (3 пары)
        gold = await self.fetch_gold_price()
        all_rates['XAU/USD'] = gold
        
        silver = await self.fetch_silver_price()
        all_rates['XAG/USD'] = silver
        
        platinum = await self.fetch_platinum_price()
        all_rates['XPT/USD'] = platinum
        
        # Индексы (2 пары)
        indices = await self.fetch_indices()
        if indices:
            all_rates.update(indices)
        
        # Товары (3 пары)
        corn = await self.fetch_corn_price()
        all_rates['CORN/USD'] = corn
        
        oil = await self.fetch_oil_prices()
        if oil:
            all_rates.update(oil)
        
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
        """Показывает меню выбора часового пояса с отметкой текущего"""
        user_id = str(chat_id)
        stats = load_user_stats()
        current_tz = stats.get(user_id, {}).get('timezone', 'Europe/Moscow')
        
        keyboard = {"inline_keyboard": []}
        
        tz_list = list(TIMEZONES.items())
        for i in range(0, len(tz_list), 2):
            row = []
            for tz_key, tz_info in tz_list[i:i+2]:
                # Добавляем галочку если это текущий часовой пояс
                check_mark = " ✅" if tz_key == current_tz else ""
                row.append({
                    "text": f"{tz_info['name']}{check_mark}", 
                    "callback_data": f"tz_{tz_key}"
                })
            keyboard["inline_keyboard"].append(row)
        
        # Отправляем сообщение без кнопки "Назад"
        await self.send_telegram_message_with_keyboard(
            chat_id,
            "🌍 <b>Выбери свой часовой пояс:</b>\n\n"
            "От этого зависит время в уведомлениях. Можно изменить в любой момент.",
            keyboard
        )    
        
    async def set_user_timezone(self, chat_id, tz_key):
        """Устанавливает часовой пояс пользователя"""
        if tz_key in TIMEZONES:
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
            await self.show_main_menu(chat_id)
        else:
            await self.send_telegram_message(chat_id, "❌ Ошибка: часовой пояс не найден")
            await self.show_main_menu(chat_id)
    
    async def show_pin_menu(self, chat_id):
        """Показывает меню для управления закреплёнными парами в два ряда"""
        rates = await self.fetch_rates()
        if not rates:
            await self.send_telegram_message(chat_id, "❌ Не удалось получить список пар")
            await self.show_main_menu(chat_id)
            return
        
        user_id = str(chat_id)
        pinned_pairs = get_user_pinned_pairs(user_id)
        
        # Собираем все пары с их данными
        all_pairs = []
        
        # Валюты (9 пар)
        currency_pairs = ['EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/RUB', 'EUR/GBP', 'USD/CAD', 'AUD/USD', 'USD/CHF', 'USD/CNY']
        for pair in currency_pairs:
            if pair in rates:
                # Эмодзи для валют
                if pair == 'EUR/USD':
                    emoji = "🇪🇺"
                elif pair == 'GBP/USD':
                    emoji = "🇬🇧"
                elif pair == 'USD/JPY':
                    emoji = "🇯🇵"
                elif pair == 'USD/RUB':
                    emoji = "🇷🇺"
                elif pair == 'EUR/GBP':
                    emoji = "🇪🇺🇬🇧"
                elif pair == 'USD/CAD':
                    emoji = "🇨🇦"
                elif pair == 'AUD/USD':
                    emoji = "🇦🇺"
                elif pair == 'USD/CHF':
                    emoji = "🇨🇭"
                elif pair == 'USD/CNY':
                    emoji = "🇨🇳"
                else:
                    emoji = "💶"
                
                pin_mark = "📌" if pair in pinned_pairs else ""
                text = f"{emoji} {pair} {pin_mark}"
                all_pairs.append({
                    'pair': pair,
                    'text': text,
                    'is_pinned': pair in pinned_pairs
                })
        
        # Металлы (3 пары)
        metals = ['XAU/USD', 'XAG/USD', 'XPT/USD']
        metal_emojis = {'XAU/USD': '🥇', 'XAG/USD': '🥈', 'XPT/USD': '🥉'}
        for pair in metals:
            if pair in rates:
                emoji = metal_emojis.get(pair, '🏅')
                pin_mark = "📌" if pair in pinned_pairs else ""
                text = f"{emoji} {pair} {pin_mark}"
                all_pairs.append({
                    'pair': pair,
                    'text': text,
                    'is_pinned': pair in pinned_pairs
                })
        
        # Крипта (5 пар)
        crypto_pairs = ['BTC/USD', 'ETH/USD', 'SOL/USD', 'XRP/USD', 'DOGE/USD']
        crypto_emojis = {
            'BTC/USD': '₿', 
            'ETH/USD': 'Ξ', 
            'SOL/USD': '◎', 
            'XRP/USD': '✪', 
            'DOGE/USD': '🐕'
        }
        for pair in crypto_pairs:
            if pair in rates:
                emoji = crypto_emojis.get(pair, '🪙')
                pin_mark = "📌" if pair in pinned_pairs else ""
                text = f"{emoji} {pair} {pin_mark}"
                all_pairs.append({
                    'pair': pair,
                    'text': text,
                    'is_pinned': pair in pinned_pairs
                })
        
        # Индексы (2 пары)
        indices = ['S&P 500', 'NASDAQ']
        index_emojis = {'S&P 500': '📈', 'NASDAQ': '📊'}
        for pair in indices:
            if pair in rates:
                emoji = index_emojis.get(pair, '📉')
                pin_mark = "📌" if pair in pinned_pairs else ""
                text = f"{emoji} {pair} {pin_mark}"
                all_pairs.append({
                    'pair': pair,
                    'text': text,
                    'is_pinned': pair in pinned_pairs
                })
        
        # Товары (3 пары)
        commodities = ['CORN/USD', 'WTI/USD', 'BRENT/USD']
        commodity_emojis = {'CORN/USD': '🌽', 'WTI/USD': '🛢️', 'BRENT/USD': '🛢️'}
        for pair in commodities:
            if pair in rates:
                emoji = commodity_emojis.get(pair, '📦')
                pin_mark = "📌" if pair in pinned_pairs else ""
                text = f"{emoji} {pair} {pin_mark}"
                all_pairs.append({
                    'pair': pair,
                    'text': text,
                    'is_pinned': pair in pinned_pairs
                })
        
        # Сортируем по названию
        all_pairs.sort(key=lambda x: x['pair'])
        
        # Создаем клавиатуру с двумя колонками
        keyboard = {"inline_keyboard": []}
        
        # Разбиваем на ряды по 2 кнопки
        for i in range(0, len(all_pairs), 2):
            row = []
            # Первая кнопка в ряду
            row.append({
                "text": all_pairs[i]['text'], 
                "callback_data": f"pin_toggle_{all_pairs[i]['pair']}"
            })
            
            # Вторая кнопка в ряду (если есть)
            if i + 1 < len(all_pairs):
                row.append({
                    "text": all_pairs[i + 1]['text'], 
                    "callback_data": f"pin_toggle_{all_pairs[i + 1]['pair']}"
                })
            
            keyboard["inline_keyboard"].append(row)
        
        # Отправляем сообщение без кнопки "Назад"
        await self.send_telegram_message_with_keyboard(
            chat_id,
            f"📌 <b>Закрепление пар</b>\n\n👇 Нажми на пару, чтобы закрепить/открепить:",
            keyboard
        )
    
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
            slogan = data.get('current_slogan', '—')
            pinned_count = len(data.get('pinned_pairs', []))
            msg += f"{i}. {name} — {data.get('interactions', 0)} сообщ.\n   📢 {slogan} | 📌 {pinned_count}\n"
        
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
    
    def format_price(self, pair, price):
        """Форматирует цену в зависимости от пары"""
        if price == 'неизвестно':
            return 'неизвестно'
        
        if pair in self.high_value_pairs:
            return f"${price:,.2f}"
        elif pair in self.low_value_pairs:
            return f"${price:.4f}"
        elif pair in self.currency_pairs:
            return f"{price:.4f}"
        else:
            return f"${price:.2f}"
    
    async def handle_pair_management(self, chat_id, pair):
        """Показывает меню управления для конкретной пары"""
        user_id = str(chat_id)
        user_alerts_list = user_alerts.get(user_id, [])
        
        active_alerts = [alert for alert in user_alerts_list 
                         if alert.get('pair') == pair and alert.get('active')]
        
        if active_alerts:
            alerts_text = ""
            for i, alert in enumerate(active_alerts, 1):
                alerts_text += f"{i}. 🎯 {alert['target']}\n"
            
            keyboard = {"inline_keyboard": []}
            
            for i, alert in enumerate(active_alerts, 1):
                keyboard["inline_keyboard"].append([
                    {"text": f"❌ {alert['target']}", 
                     "callback_data": f"delete_specific_{pair}_{i}"}
                ])
            
            keyboard["inline_keyboard"].append([
                {"text": "➕ Добавить цель", "callback_data": f"add_{pair}"}
            ])
            
            # Кнопка "Назад" УБРАНА!
            
            await self.send_telegram_message_with_keyboard(
                chat_id,
                f"📊 {pair}\n\n"
                f"Всего алертов: {len(active_alerts)}\n\n"
                f"{alerts_text}",
                keyboard
            )
        else:
            # Получаем текущую цену для отображения при создании
            rates = await self.fetch_rates()
            current_price = rates.get(pair, 'неизвестно')
            price_str = self.format_price(pair, current_price)
            
            self.alert_states[str(chat_id)] = {'pair': pair, 'step': 'waiting_price'}
            
            await self.send_telegram_message(
                chat_id,
                f"Создать алерт для {pair}\n"
                f"💰 Текущая цена: {price_str}\n\n"
                f"📝 Введи целевую цену:"
            )
        
    async def show_main_menu(self, chat_id):
        """Главное меню со слоганом, индикаторами алертов и закреплений"""
        try:
            rates = await self.fetch_rates()
            if not rates:
                # Если нет курсов, показываем упрощённое меню
                keyboard = {
                    "inline_keyboard": [
                        [{"text": "📩 Связь", "callback_data": "collaboration"}],
                        [{"text": "🌍 Часовой пояс", "callback_data": "show_timezone"}],
                        [{"text": "📌 Закрепить", "callback_data": "show_pin_menu"}]
                    ]
                }
                slogan = get_user_slogan(chat_id)
                await self.send_telegram_message_with_keyboard(chat_id, slogan, keyboard)
                return    
                
        user_id = str(chat_id)
        user_alerts_list = user_alerts.get(user_id, [])
        pinned_pairs = get_user_pinned_pairs(user_id)
        
        def get_alert_indicator(count):
            if count == 0:
                return ""
            else:
                return f" {number_to_emoji(count)}"
        
        def get_pin_indicator(pair):
            return "📌" if pair in pinned_pairs else ""
        
        # Собираем все пары с их данными
        all_pairs = []
        
        # Валюты (9 пар)
        currency_pairs = ['EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/RUB', 'EUR/GBP', 'USD/CAD', 'AUD/USD', 'USD/CHF', 'USD/CNY']
        for pair in currency_pairs:
            if pair in rates:
                alert_count = sum(1 for alert in user_alerts_list 
                                  if alert.get('pair') == pair and alert.get('active'))
                alert_indicator = get_alert_indicator(alert_count)
                pin = get_pin_indicator(pair)
                
                # Эмодзи для валют
                if pair == 'EUR/USD':
                    emoji = "🇪🇺"
                elif pair == 'GBP/USD':
                    emoji = "🇬🇧"
                elif pair == 'USD/JPY':
                    emoji = "🇯🇵"
                elif pair == 'USD/RUB':
                    emoji = "🇷🇺"
                elif pair == 'EUR/GBP':
                    emoji = "🇪🇺🇬🇧"
                elif pair == 'USD/CAD':
                    emoji = "🇨🇦"
                elif pair == 'AUD/USD':
                    emoji = "🇦🇺"
                elif pair == 'USD/CHF':
                    emoji = "🇨🇭"
                elif pair == 'USD/CNY':
                    emoji = "🇨🇳"
                else:
                    emoji = "💶"
                
                text = f"{emoji} {pair}{alert_indicator}{pin}"
                all_pairs.append({
                    'pair': pair,
                    'text': text,
                    'is_pinned': pair in pinned_pairs,
                    'category': 'currency',
                    'sort_key': pair
                })
        
        # Металлы (3 пары)
        metals = ['XAU/USD', 'XAG/USD', 'XPT/USD']
        metal_emojis = {'XAU/USD': '🥇', 'XAG/USD': '🥈', 'XPT/USD': '🥉'}
        for pair in metals:
            if pair in rates:
                alert_count = sum(1 for alert in user_alerts_list 
                                  if alert.get('pair') == pair and alert.get('active'))
                alert_indicator = get_alert_indicator(alert_count)
                pin = get_pin_indicator(pair)
                emoji = metal_emojis.get(pair, '🏅')
                
                text = f"{emoji} {pair}{alert_indicator}{pin}"
                all_pairs.append({
                    'pair': pair,
                    'text': text,
                    'is_pinned': pair in pinned_pairs,
                    'category': 'metal',
                    'sort_key': pair
                })
        
        # Крипта (5 пар)
        crypto_pairs = ['BTC/USD', 'ETH/USD', 'SOL/USD', 'XRP/USD', 'DOGE/USD']
        crypto_emojis = {
            'BTC/USD': '₿', 
            'ETH/USD': 'Ξ', 
            'SOL/USD': '◎', 
            'XRP/USD': '✪', 
            'DOGE/USD': '🐕'
        }
        for pair in crypto_pairs:
            if pair in rates:
                alert_count = sum(1 for alert in user_alerts_list 
                                  if alert.get('pair') == pair and alert.get('active'))
                alert_indicator = get_alert_indicator(alert_count)
                pin = get_pin_indicator(pair)
                emoji = crypto_emojis.get(pair, '🪙')
                
                text = f"{emoji} {pair}{alert_indicator}{pin}"
                all_pairs.append({
                    'pair': pair,
                    'text': text,
                    'is_pinned': pair in pinned_pairs,
                    'category': 'crypto',
                    'sort_key': pair
                })
        
        # Индексы (2 пары)
        indices = ['S&P 500', 'NASDAQ']
        index_emojis = {'S&P 500': '📈', 'NASDAQ': '📊'}
        for pair in indices:
            if pair in rates:
                alert_count = sum(1 for alert in user_alerts_list 
                                  if alert.get('pair') == pair and alert.get('active'))
                alert_indicator = get_alert_indicator(alert_count)
                pin = get_pin_indicator(pair)
                emoji = index_emojis.get(pair, '📉')
                
                text = f"{emoji} {pair}{alert_indicator}{pin}"
                all_pairs.append({
                    'pair': pair,
                    'text': text,
                    'is_pinned': pair in pinned_pairs,
                    'category': 'index',
                    'sort_key': pair
                })
        
        # Товары (3 пары)
        commodities = ['CORN/USD', 'WTI/USD', 'BRENT/USD']
        commodity_emojis = {'CORN/USD': '🌽', 'WTI/USD': '🛢️', 'BRENT/USD': '🛢️'}
        for pair in commodities:
            if pair in rates:
                alert_count = sum(1 for alert in user_alerts_list 
                                  if alert.get('pair') == pair and alert.get('active'))
                alert_indicator = get_alert_indicator(alert_count)
                pin = get_pin_indicator(pair)
                emoji = commodity_emojis.get(pair, '📦')
                
                text = f"{emoji} {pair}{alert_indicator}{pin}"
                all_pairs.append({
                    'pair': pair,
                    'text': text,
                    'is_pinned': pair in pinned_pairs,
                    'category': 'commodity',
                    'sort_key': pair
                })
        
        # Сортируем: сначала закрепленные, потом остальные
        pinned_items = [p for p in all_pairs if p['is_pinned']]
        regular_items = [p for p in all_pairs if not p['is_pinned']]
        
        # Сортируем каждую группу по названию
        pinned_items.sort(key=lambda x: x['sort_key'])
        regular_items.sort(key=lambda x: x['sort_key'])
        
        # Объединяем
        sorted_pairs = pinned_items + regular_items
        
        # Создаем клавиатуру с двумя колонками
        keyboard = {"inline_keyboard": []}
        
        # Разбиваем на ряды по 2 кнопки
        for i in range(0, len(sorted_pairs), 2):
            row = []
            # Первая кнопка в ряду
            row.append({
                "text": sorted_pairs[i]['text'], 
                "callback_data": f"manage_{sorted_pairs[i]['pair']}"
            })
            
            # Вторая кнопка в ряду (если есть)
            if i + 1 < len(sorted_pairs):
                row.append({
                    "text": sorted_pairs[i + 1]['text'], 
                    "callback_data": f"manage_{sorted_pairs[i + 1]['pair']}"
                })
            
            keyboard["inline_keyboard"].append(row)
        
        # Добавляем нижнюю панель с функциями
        keyboard["inline_keyboard"].append([
            {"text": "📩 Связь", "callback_data": "collaboration"},
            {"text": "🌍 Часовой пояс", "callback_data": "show_timezone"},
            {"text": "📌 Закрепить", "callback_data": "show_pin_menu"}
        ])
        
        slogan = get_user_slogan(chat_id)
        
        await self.send_telegram_message_with_keyboard(chat_id, slogan, keyboard)
    
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
            
            await self.send_telegram_message(
                chat_id,
                f"✅ Алерт для {pair} создан!\n\n"
                f"🎯 Цель: {target}"
            )
            
            await self.show_main_menu(chat_id)
            
        except ValueError:
            await self.send_telegram_message(chat_id, "❌ Это не число! Введи цену (например: 1.10)")
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
            msg += f"{number_to_emoji(i)} {status} {pair} = {target}\n"
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
            
            if text == '/pin':
                await self.show_pin_menu(chat_id)
                return
            
            if str(chat_id) in self.alert_states:
                await self.handle_alert_input(chat_id, text)
                return
            
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
                
            elif data == "show_pin_menu":
                await self.show_pin_menu(chat_id)
                
            elif data.startswith("tz_"):
                tz_key = data.replace("tz_", "")
                await self.set_user_timezone(chat_id, tz_key)
                
            elif data.startswith("pin_toggle_"):
                pair = data.replace("pin_toggle_", "")
                user_id = str(chat_id)
                stats = load_user_stats()
                
                if user_id not in stats:
                    stats[user_id] = {'pinned_pairs': []}
                
                pinned_pairs = stats[user_id].get('pinned_pairs', [])
                
                if pair in pinned_pairs:
                    pinned_pairs = [p for p in pinned_pairs if p != pair]
                else:
                    pinned_pairs.append(pair)
                
                update_user_stats(chat_id, '', '', '', pinned_pairs=pinned_pairs)
                
                # Сразу возвращаемся в главное меню
                await self.show_main_menu(chat_id)
                
            elif data.startswith("manage_"):
                pair = data.replace("manage_", "")
                await self.handle_pair_management(chat_id, pair)
                
            elif data.startswith("delete_specific_"):
                try:
                    parts = data.replace("delete_specific_", "").rsplit("_", 1)
                    pair = parts[0]
                    alert_num = int(parts[1]) - 1
                    
                    user_id = str(chat_id)
                    if user_id in user_alerts:
                        pair_alerts = [alert for alert in user_alerts[user_id] 
                                       if alert.get('pair') == pair and alert.get('active')]
                        
                        if 0 <= alert_num < len(pair_alerts):
                            target_alert = pair_alerts[alert_num]
                            user_alerts[user_id] = [a for a in user_alerts[user_id] 
                                                     if not (a.get('pair') == pair and 
                                                            a.get('target') == target_alert['target'] and 
                                                            a.get('active'))]
                            save_user_alerts(user_alerts)
                            
                            remaining_alerts = [a for a in user_alerts[user_id] 
                                               if a.get('pair') == pair and a.get('active')]
                            
                            if not remaining_alerts:
                                await self.send_telegram_message(chat_id, f"✅ Все алерты для {pair} удалены")
                                await self.show_main_menu(chat_id)
                                return
                except Exception as e:
                    logger.error(f"Delete specific error: {e}")
                
                # Если остались алерты, показываем управление парой
                await self.handle_pair_management(chat_id, pair)
                
            elif data.startswith("delete_all_"):
                pair = data.replace("delete_all_", "")
                user_id = str(chat_id)
                if user_id in user_alerts:
                    old_count = len([a for a in user_alerts[user_id] 
                                     if a.get('pair') == pair and a.get('active')])
                    user_alerts[user_id] = [a for a in user_alerts[user_id] 
                                             if not (a.get('pair') == pair and a.get('active'))]
                    save_user_alerts(user_alerts)
                    logger.info(f"Удалено {old_count} алертов для {pair} у пользователя {user_id}")
                    
                    await self.send_telegram_message(chat_id, f"✅ Все алерты для {pair} удалены")
                    await self.show_main_menu(chat_id)
                    return
                    
            elif data.startswith("add_"):
                pair = data.replace("add_", "")
                
                # Получаем текущую цену для отображения при создании
                rates = await self.fetch_rates()
                current_price = rates.get(pair, 'неизвестно')
                price_str = self.format_price(pair, current_price)
                
                self.alert_states[str(chat_id)] = {'pair': pair, 'step': 'waiting_price'}
                await self.send_telegram_message(
                    chat_id,
                    f"Создать алерт для {pair}\n"
                    f"💰 Текущая цена: {price_str}\n\n"
                    f"📝 Введи целевую цену:"
                )
                
            elif data == "collaboration":
                collab_text = (
                    "📩 <b>Обратная связь</b>\n\n"
                    "📈 Нет какой-то валютной пары в списке?\n"
                    "✉️ Напиши @Maranafa2023 — добавим!\n\n"
                    "Спасибо, что пользуетесь ботом! 🚀"
                )
                
                ok_keyboard = {
                    "inline_keyboard": [
                        [{"text": "✅ ОК", "callback_data": "main_menu"}]
                    ]
                }
                
                await self.send_telegram_message_with_keyboard(
                    chat_id, 
                    collab_text, 
                    ok_keyboard
                )            
                            
            elif data == "cancel_alert":
                if str(chat_id) in self.alert_states:
                    del self.alert_states[str(chat_id)]
                await self.send_telegram_message(chat_id, "❌ Создание отменено")
                await self.show_main_menu(chat_id)
                
            elif data.startswith("delete_"):
                try:
                    num = int(data.replace("delete_", "")) - 1
                    user_id = str(chat_id)
                    if user_id in user_alerts and 0 <= num < len(user_alerts[user_id]):
                        user_alerts[user_id].pop(num)
                        save_user_alerts(user_alerts)
                        await self.send_telegram_message(chat_id, f"✅ Алерт удален")
                        # Сразу показываем главное меню
                        await self.show_main_menu(chat_id)
                except Exception as e:
                    logger.error(f"Delete error: {e}")
                    await self.show_main_menu(chat_id)  
                    
        except Exception as e:
            logger.error(f"Callback error: {e}")
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
                
                if pair in ['BTC/USD', 'ETH/USD', 'XAU/USD', 'XPT/USD', 'S&P 500', 'NASDAQ']:
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
                
                elif pair in ['DOGE/USD', 'XRP/USD']:
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
        logger.info(f"📊 Пары: валюты (9) + металлы (3) + крипта (5) + индексы (2) + товары (3) = 22 пары")
        logger.info(f"🎯 Точность: максимальная")
        logger.info(f"🌍 Поддержка часовых поясов: {len(TIMEZONES)} городов")
        logger.info(f"🔄 Слоганы меняются раз в 24 часа с учётом времени года")
        logger.info(f"📌 Закрепленные пары отмечены 📌 в главном меню")
        logger.info(f"🔢 Для 10+ алертов используются составные эмодзи-цифры")
        logger.info(f"💰 При создании алерта показывается текущая цена")
        logger.info(f"📱 Главное меню: без цен, в два ряда, с флагами и эмодзи")
        logger.info(f"📌 Меню закрепления: в два ряда, с флагами и эмодзи (без кнопки назад)")
        logger.info(f"✅ В меню часовых поясов галочка у выбранного")
        logger.info(f"🌞❄️🌸🍂 Сезонные слоганы: лето, зима, весна, осень")
        if YFINANCE_AVAILABLE:
            logger.info(f"📈 Индексы и нефть: yfinance доступен")
        else:
            logger.info(f"📈 Индексы и нефть: yfinance не установлен, используются другие источники")
        
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
import asyncio
import aiohttp
import logging
from datetime import datetime
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

# ===== НАСТРОЙКА ДОСТУПА =====
ALLOWED_USER_IDS = [
    5799391012,  # ЗАМЕНИ НА СВОЙ ID
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

def update_user_stats(chat_id, username, first_name, last_name, pair=None):
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
            'pairs': []
        }
    
    stats[user_id]['last_seen'] = datetime.now().isoformat()
    stats[user_id]['interactions'] += 1
    
    if pair:
        stats[user_id]['pairs'].append(pair)
        if len(stats[user_id]['pairs']) > 50:
            stats[user_id]['pairs'] = stats[user_id]['pairs'][-50:]
    
    save_user_stats(stats)
    return stats[user_id]

# Глобальные переменные
user_alerts = load_user_alerts()
last_notifications = {}

# Московский часовой пояс (только для внутренних логов)
MSK_TZ = ZoneInfo('Europe/Moscow')

class CurrencyMonitor:
    def __init__(self):
        self.session = None
        self.last_update_id = 0
        self.alert_states = {}
        self.last_successful_rates = {
            'EUR/USD': 1.08,
            'GBP/USD': 1.26,
            'USD/JPY': 155.0,
            'USD/RUB': 90.0,
            'XAU/USD': 5160.0,
            'BTC/USD': 67000.0,
            'ETH/USD': 1950.0,
            'SOL/USD': 84.0,
            'BNB/USD': 610.0,
            'LINK/USD': 8.6,
            'TON/USD': 1.35,
            'XRP/USD': 1.40,
            'DOGE/USD': 0.098,
            'AVAX/USD': 9.1,
            'EUR/GBP': 0.87
        }
    
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
    
    def parse_vn_gold(self, data):
        """Парсит цену из вьетнамского API (запасной вариант)"""
        try:
            if data and 'data' in data and data['data']:
                vnd_price = float(data['data'][0]['buy'])
                usd_price = vnd_price / 25400
                return usd_price
        except:
            return None
        return None
    
    async def fetch_gold_price(self):
        """Получает цену золота из нескольких рабочих источников"""
        try:
            session = await self.get_session()
            
            sources = [
                {
                    # Источник 1: gold-api.com (самый надежный, без ключа)
                    'name': 'Gold-API.com',
                    'url': 'https://api.gold-api.com/price/XAU',
                    'parser': lambda data: float(data['price']) if data and 'price' in data else None
                },
                {
                    # Источник 2: metals-api.com (нужен бесплатный ключ)
                    'name': 'Metals-API',
                    'url': 'https://api.metals-api.com/v1/latest?access_key=free&base=USD&symbols=XAU',
                    'parser': lambda data: 1.0 / float(data['rates']['XAU']) if data and 'rates' in data and 'XAU' in data['rates'] else None
                },
                {
                    # Источник 3: FreeGoldPrice.org (бесплатно, без ключа)
                    'name': 'FreeGoldPrice',
                    'url': 'https://freegoldprice.org/api/current',
                    'parser': lambda data: float(data['gold_price_usd']) if data and 'gold_price_usd' in data else None
                },
                {
                    # Источник 4: vnappmob (азиатский сервер, быстрый)
                    'name': 'VNAppMob',
                    'url': 'https://api.vnappmob.com/api/v2/gold/sjc',
                    'parser': self.parse_vn_gold
                }
            ]
            
            for source in sources:
                try:
                    logger.info(f"Пробуем источник: {source['name']}")
                    async with session.get(source['url'], timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            if source['name'] == 'VNAppMob':
                                price = source['parser'](data)
                            else:
                                price = source['parser'](data)
                            
                            if price and price > 1000 and price < 10000:
                                logger.info(f"✅ Золото: ${price:.2f}/унция (источник: {source['name']})")
                                self.last_successful_rates['XAU/USD'] = price
                                return price
                            else:
                                logger.warning(f"⚠️ {source['name']} вернул некорректную цену: {price}")
                        else:
                            logger.warning(f"⚠️ {source['name']} вернул статус {response.status}")
                except Exception as e:
                    logger.warning(f"❌ {source['name']} failed: {e}")
                    continue
            
            # Запасной вариант — парсинг HTML
            try:
                url = "https://www.goldprice.org/live-gold-price"
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        html = await response.text()
                        match = re.search(r'XAUUSD.*?(\d+\.?\d*)', html)
                        if match:
                            price = float(match.group(1))
                            if 1000 < price < 10000:
                                logger.info(f"✅ Золото: ${price:.2f}/унция (источник: GoldPrice.org HTML)")
                                self.last_successful_rates['XAU/USD'] = price
                                return price
            except Exception as e:
                logger.warning(f"❌ HTML parsing error: {e}")
            
        except Exception as e:
            logger.error(f"Gold API error: {e}")
        
        # Если всё упало, возвращаем кэш
        logger.warning("⚠️ Все источники золота недоступны, использую кэш")
        return self.last_successful_rates.get('XAU/USD', 5160.0)
    
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
        
        fiat = await self.fetch_from_fiat_api()
        if fiat:
            all_rates.update(fiat)
        
        crypto = await self.fetch_from_binance()
        if crypto:
            all_rates.update(crypto)
        
        gold_price = await self.fetch_gold_price()
        all_rates['XAU/USD'] = gold_price
        
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
    
    async def show_stats(self, chat_id):
        """Показывает статистику использования бота (только для админа)"""
        if not self.is_admin(chat_id):
            await self.send_telegram_message(chat_id, "❌ У тебя нет доступа к статистике")
            return
        
        stats = load_user_stats()
        
        if not stats:
            await self.send_telegram_message(chat_id, "📊 Статистика пока пуста")
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
    
    async def show_main_menu(self, chat_id):
        """Главное меню с кнопкой сотрудничества"""
        keyboard = {
            "inline_keyboard": [
                [{"text": "💰 Добавить алерт", "callback_data": "start_alert"}],
                [{"text": "📋 Мои алерты", "callback_data": "show_alerts"}],
                [{"text": "📊 Текущие курсы", "callback_data": "show_rates"}],
                [{"text": "🤝 Сотрудничество", "callback_data": "collaboration"}]
            ]
        }
        await self.send_telegram_message_with_keyboard(chat_id, "🔍 Выбери действие:", keyboard)
    
    async def start_alert_creation(self, chat_id):
        self.alert_states[str(chat_id)] = {'step': 'pair'}
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "💶 EUR/USD", "callback_data": "pair_EUR/USD"}],
                [{"text": "💷 GBP/USD", "callback_data": "pair_GBP/USD"}],
                [{"text": "💵 USD/JPY", "callback_data": "pair_USD/JPY"}],
                [{"text": "🏅 XAU/USD", "callback_data": "pair_XAU/USD"}],
                [{"text": "💶💷 EUR/GBP", "callback_data": "pair_EUR/GBP"}],
                [{"text": "———— КРИПТОВАЛЮТЫ ————", "callback_data": "noop"}],
                [{"text": "₿ BTC/USD", "callback_data": "pair_BTC/USD"}],
                [{"text": "🟦 ETH/USD", "callback_data": "pair_ETH/USD"}],
                [{"text": "🟪 SOL/USD", "callback_data": "pair_SOL/USD"}],
                [{"text": "🟨 BNB/USD", "callback_data": "pair_BNB/USD"}],
                [{"text": "🔗 LINK/USD", "callback_data": "pair_LINK/USD"}],
                [{"text": "💎 TON/USD", "callback_data": "pair_TON/USD"}],
                [{"text": "⚡️ XRP/USD", "callback_data": "pair_XRP/USD"}],
                [{"text": "🐕 DOGE/USD", "callback_data": "pair_DOGE/USD"}],
                [{"text": "🏔 AVAX/USD", "callback_data": "pair_AVAX/USD"}],
                [{"text": "◀️ Отмена", "callback_data": "cancel_alert"}]
            ]
        }
        
        await self.send_telegram_message_with_keyboard(chat_id, "📈 Выбери валютную пару:", keyboard)
    
    async def handle_alert_input(self, chat_id, text):
        try:
            text = text.replace(',', '.')
            target = float(text)
            
            if str(chat_id) not in self.alert_states:
                await self.send_telegram_message(chat_id, "❌ Ошибка: начни сначала /start")
                return
                
            state = self.alert_states[str(chat_id)]
            if 'pair' not in state:
                await self.send_telegram_message(chat_id, "❌ Ошибка: выбери пару сначала")
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
                f"✅ Алерт создан!\n\n"
                f"📊 {pair}\n"
                f"🎯 Цель: {target}"
            )
            await self.show_main_menu(chat_id)
            
        except ValueError:
            await self.send_telegram_message(chat_id, "❌ Это не число! Введи цену (например: 1.10)")
        except Exception as e:
            logger.error(f"Error in alert input: {e}")
            await self.send_telegram_message(chat_id, "❌ Ошибка при создании алерта")
    
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
            
            if str(chat_id) in self.alert_states:
                await self.handle_alert_input(chat_id, text)
                return
            
            if text == '/alert':
                await self.start_alert_creation(chat_id)
            elif text == '/myalerts':
                await self.list_alerts(chat_id)
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
                await self.show_main_menu(chat_id)
            elif data == "start_alert":
                await self.start_alert_creation(chat_id)
            elif data == "show_alerts":
                await self.list_alerts(chat_id)
            elif data == "show_rates":
                rates = await self.fetch_rates()
                if rates:
                    msg = "📊 Текущие курсы:\n\n"
                    for pair, rate in sorted(rates.items()):
                        if pair in ['BTC/USD', 'ETH/USD']:
                            msg += f"{pair}: ${rate:,.2f}\n"
                        elif pair == 'XAU/USD':
                            msg += f"{pair}: ${rate:,.2f}\n"
                        elif pair in ['SOL/USD', 'BNB/USD', 'AVAX/USD', 'LINK/USD']:
                            msg += f"{pair}: ${rate:.2f}\n"
                        elif pair in ['XRP/USD', 'DOGE/USD', 'TON/USD']:
                            msg += f"{pair}: ${rate:.4f}\n"
                        else:
                            msg += f"{pair}: {rate:.4f}\n"
                    
                    keyboard = {
                        "inline_keyboard": [
                            [{"text": "◀️ Назад", "callback_data": "main_menu"}]
                        ]
                    }
                    await self.send_telegram_message_with_keyboard(chat_id, msg, keyboard)
                else:
                    await self.send_telegram_message(chat_id, "❌ Ошибка получения курсов")
            elif data == "collaboration":
                collab_text = (
                    "🤝 <b>СОТРУДНИЧЕСТВО</b>\n\n"
                    "📊 Нравится бот? Хочешь такой же для своих целей?\n"
                    "💎 Помогу с разработкой, настройкой, доработкой\n\n"
                    "📩 Пиши: @Maranafa2023 - обсудим детали"
                )
                await self.send_telegram_message(chat_id, collab_text)
            elif data == "cancel_alert":
                if str(chat_id) in self.alert_states:
                    del self.alert_states[str(chat_id)]
                await self.send_telegram_message(chat_id, "❌ Создание отменено")
                await self.show_main_menu(chat_id)
            elif data.startswith("pair_") and data != "pair_noop" and data != "noop":
                pair = data.replace("pair_", "")
                if str(chat_id) in self.alert_states:
                    self.alert_states[str(chat_id)]['pair'] = pair
                    
                    hints = {
                        'EUR/USD': '1.10', 'GBP/USD': '1.30', 'USD/JPY': '150',
                        'EUR/GBP': '0.87', 'XAU/USD': '5160', 'BTC/USD': '67000',
                        'ETH/USD': '1950', 'SOL/USD': '84', 'BNB/USD': '610',
                        'LINK/USD': '8.6', 'TON/USD': '1.35', 'XRP/USD': '1.40',
                        'DOGE/USD': '0.098', 'AVAX/USD': '9.1'
                    }
                    hint = hints.get(pair, '1.0')
                    
                    await self.send_telegram_message(
                        chat_id,
                        f"💰 Пара: {pair}\n\n📝 Введи целевую цену:\nНапример: {hint}"
                    )
            elif data.startswith("delete_"):
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
                    
        except Exception as e:
            logger.error(f"Callback error: {e}")
    
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
        
        for user_id, alerts in user_alerts.items():
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
                
                if pair in ['BTC/USD', 'ETH/USD', 'XAU/USD']:
                    if abs(current - target) / target < 0.0001:
                        msg = (
                            f"🎯 <b>ЦЕЛЬ ДОСТИГНУТА!</b>\n\n"
                            f"📊 {pair}\n"
                            f"🎯 Цель: {target:.2f}\n"
                            f"💰 Текущий: {current:.2f}"
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
                            f"💰 Текущий: {current:.4f}"
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
                            f"💰 Текущий: {current:.5f}"
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
        logger.info(f"📊 Пары: фиат + золото (4 источника) + криптовалюты")
        logger.info(f"🎯 Точность: максимальная")
        
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
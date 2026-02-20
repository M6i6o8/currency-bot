import asyncio
import aiohttp
import logging
from datetime import datetime
import os
import json
import sys
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация Telegram
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

# ===== НАСТРОЙКА ДОСТУПА =====
ALLOWED_USER_IDS = [
    123456789,  # Замени на свой ID
]

DEFAULT_MODE = "private"
# ============================

if len(sys.argv) > 1:
    mode_arg = sys.argv[1].lower()
    PRIVATE_MODE = (mode_arg == "private")
else:
    PRIVATE_MODE = (DEFAULT_MODE == "private")

# Файл для хранения пользовательских настроек
USER_ALERTS_FILE = "user_alerts.json"

def load_user_alerts():
    """Загружает алерты и конвертирует старый формат в новый"""
    if os.path.exists(USER_ALERTS_FILE):
        with open(USER_ALERTS_FILE, 'r', encoding='utf-8') as f:
            alerts = json.load(f)
            
        # Конвертируем старые алерты в новый формат
        for user_id, user_alerts in alerts.items():
            for alert in user_alerts:
                # Если есть target_price, преобразуем в target
                if 'target_price' in alert and 'target' not in alert:
                    alert['target'] = alert['target_price']
                # Если есть created_at, преобразуем в created
                if 'created_at' in alert and 'created' not in alert:
                    alert['created'] = alert['created_at']
                    
        return alerts
    return {}

def save_user_alerts(alerts):
    """Сохраняет пользовательские алерты в файл"""
    with open(USER_ALERTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(alerts, f, indent=2, ensure_ascii=False)

# Глобальная переменная для хранения алертов
user_alerts = load_user_alerts()
last_notifications = {}

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
            'XAU/USD': 2000.0,
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
                    
                    # Добавляем EUR/GBP
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
        """Получает все курсы из всех источников"""
        all_rates = {}
        
        # 1. Фиатные валюты
        fiat = await self.fetch_from_fiat_api()
        if fiat:
            all_rates.update(fiat)
        
        # 2. Криптовалюты (с Binance) - включая BTC
        crypto = await self.fetch_from_binance()
        if crypto:
            all_rates.update(crypto)
        
        # 3. Добавляем золото
        all_rates['XAU/USD'] = self.last_successful_rates.get('XAU/USD', 2000.0)
        
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
    
    async def show_main_menu(self, chat_id):
        keyboard = {
            "inline_keyboard": [
                [{"text": "💰 Добавить алерт", "callback_data": "start_alert"}],
                [{"text": "📋 Мои алерты", "callback_data": "show_alerts"}],
                [{"text": "❓ Помощь", "callback_data": "show_help"}],
                [{"text": "📊 Текущие курсы", "callback_data": "show_rates"}]
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
                'active': True,
                'created': datetime.now().strftime('%H:%M:%S')
            }
            
            user_alerts[user_id].append(alert)
            save_user_alerts(user_alerts)
            
            del self.alert_states[str(chat_id)]
            
            await self.send_telegram_message(
                chat_id,
                f"✅ Алерт создан!\n\n"
                f"📊 {pair}\n"
                f"🎯 Цель: {target}\n\n"
                f"⚡️ Проверка каждые 10 секунд"
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
            # Пробуем получить target из разных возможных полей
            target = alert.get('target') or alert.get('target_price') or '?'
            pair = alert.get('pair', '?')
            msg += f"{i}. {status} {pair} = {target}\n"
            keyboard["inline_keyboard"].append(
                [{"text": f"❌ Удалить {i}", "callback_data": f"delete_{i}"}]
            )
        
        keyboard["inline_keyboard"].append([{"text": "◀️ Назад", "callback_data": "main_menu"}])
        await self.send_telegram_message_with_keyboard(chat_id, msg, keyboard)
    
    async def show_help(self, chat_id):
        help_text = (
            "📚 <b>Как пользоваться:</b>\n\n"
            "1️⃣ Нажми <b>«💰 Добавить алерт»</b>\n"
            "2️⃣ Выбери пару\n"
            "3️⃣ Введи целевую цену\n\n"
            "⚡️ <b>Пары с низким спредом:</b>\n"
            "• Фиат: EUR/USD, GBP/USD, USD/JPY, EUR/GBP\n"
            "• Золото: XAU/USD\n"
            "• Крипто: BTC, ETH, SOL, BNB, LINK, TON, XRP, DOGE, AVAX\n\n"
            "🔹 <b>/start</b> - главное меню"
        )
        await self.send_telegram_message(chat_id, help_text)
    
    async def handle_telegram_commands(self, update):
        try:
            if 'message' not in update:
                return
            
            msg = update['message']
            chat_id = msg['chat']['id']
            text = msg.get('text', '')
            
            if not self.is_user_allowed(chat_id):
                logger.info(f"⛔ Запрещен: {chat_id}")
                return
            
            if text in ['/start', '/menu']:
                if str(chat_id) in self.alert_states:
                    del self.alert_states[str(chat_id)]
                await self.show_main_menu(chat_id)
                return
            
            if str(chat_id) in self.alert_states:
                await self.handle_alert_input(chat_id, text)
                return
            
            if text == '/alert':
                await self.start_alert_creation(chat_id)
            elif text == '/myalerts':
                await self.list_alerts(chat_id)
            elif text == '/help':
                await self.show_help(chat_id)
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
            elif data == "show_help":
                await self.show_help(chat_id)
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
                    
                    msg += f"\n⏱ {datetime.now().strftime('%H:%M:%S')}"
                    
                    keyboard = {
                        "inline_keyboard": [
                            [{"text": "◀️ Назад", "callback_data": "main_menu"}]
                        ]
                    }
                    await self.send_telegram_message_with_keyboard(chat_id, msg, keyboard)
                else:
                    await self.send_telegram_message(chat_id, "❌ Ошибка получения курсов")
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
                        'EUR/GBP': '0.87', 'XAU/USD': '2000', 'BTC/USD': '67000',
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
        """Проверяет достижение целевых цен с максимальной точностью"""
        notifications = []
        now = datetime.now()
        
        for user_id, alerts in user_alerts.items():
            for alert in alerts:
                # Пропускаем неактивные
                if not alert.get('active', False):
                    continue
                
                # Получаем target из разных возможных полей
                target = alert.get('target') or alert.get('target_price')
                if target is None:
                    continue  # Пропускаем алерты без цели
                    
                pair = alert.get('pair')
                if not pair or pair not in rates:
                    continue
                
                current = rates[pair]
                
                # ===== МАКСИМАЛЬНАЯ ТОЧНОСТЬ =====
                # Для всех пар используем минимальный допуск
                if pair in ['BTC/USD', 'ETH/USD', 'XAU/USD']:
                    # Для дорогих активов - допуск 0.01% (максимальная точность)
                    if abs(current - target) / target < 0.0001:  # 0.01%
                        msg = (
                            f"🎯 <b>ЦЕЛЬ ДОСТИГНУТА!</b>\n\n"
                            f"📊 {pair}\n"
                            f"🎯 Цель: {target:.2f}\n"
                            f"💰 Текущий: {current:.2f}\n"
                            f"⏱ {now.strftime('%H:%M:%S')}"
                        )
                        notifications.append((int(user_id), msg))
                        alert['active'] = False
                        save_user_alerts(user_alerts)
                        logger.info(f"Цель {pair}: {current:.2f} (цель: {target:.2f})")
                
                elif pair in ['DOGE/USD', 'XRP/USD', 'TON/USD']:
                    # Для дешевых монет - допуск 0.0001 (максимальная точность)
                    if abs(current - target) <= 0.0001:  # 0.0001 допуск
                        msg = (
                            f"🎯 <b>ЦЕЛЬ ДОСТИГНУТА!</b>\n\n"
                            f"📊 {pair}\n"
                            f"🎯 Цель: {target:.4f}\n"
                            f"💰 Текущий: {current:.4f}\n"
                            f"⏱ {now.strftime('%H:%M:%S')}"
                        )
                        notifications.append((int(user_id), msg))
                        alert['active'] = False
                        save_user_alerts(user_alerts)
                        logger.info(f"Цель {pair}: {current:.4f} (цель: {target:.4f})")
                
                else:
                    # Для всех остальных пар (EUR/USD, GBP/USD, EUR/GBP, USD/JPY, SOL/USD, BNB/USD, LINK/USD, AVAX/USD)
                    # Используем допуск 0.00001 (максимальная точность до 5 знака)
                    if abs(current - target) <= 0.00005:  # 0.00005 допуск
                        msg = (
                            f"🎯 <b>ЦЕЛЬ ДОСТИГНУТА!</b>\n\n"
                            f"📊 {pair}\n"
                            f"🎯 Цель: {target:.5f}\n"
                            f"💰 Текущий: {current:.5f}\n"
                            f"⏱ {now.strftime('%H:%M:%S')}"
                        )
                        notifications.append((int(user_id), msg))
                        alert['active'] = False
                        save_user_alerts(user_alerts)
                        logger.info(f"Цель {pair}: {current:.5f} (цель: {target:.5f})")
        
        return notifications
    
    async def check_rates_task(self, interval=10):
        """Задача для проверки курсов"""
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
        """Задача для проверки команд"""
        while True:
            try:
                await self.get_updates()
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Commands task error: {e}")
                await asyncio.sleep(5)
    
    async def run(self):
        """Запускает обе задачи параллельно"""
        mode = "ОТКРЫТЫЙ" if not PRIVATE_MODE else "ПРИВАТНЫЙ"
        logger.info(f"🚀 ЗАПУСК БОТА [{mode} РЕЖИМ]")
        logger.info(f"⚡️ Проверка: каждые 10 секунд")
        logger.info(f"📊 Пары: фиат + золото + 9 криптовалют (включая BTC)")
        logger.info(f"🎯 Точность: максимальная (0.00005 для валют, 0.01% для крипты)")
        
        try:
            await asyncio.gather(
                self.check_rates_task(interval=10),
                self.check_commands_task(interval=2)
            )
        except KeyboardInterrupt:
            logger.info("⏹ Остановлено")
        finally:
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
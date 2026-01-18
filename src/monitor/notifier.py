import requests
import json
from datetime import datetime
from ..config import settings
from ..utils.logger import log

class DiscordNotifier:
    def __init__(self):
        self.webhook_url = settings.get("discord_webhook_url")
        if not self.webhook_url:
            log.warning("No Discord Webhook URL found in config. Notifications will be disabled.")
    
    def _send(self, payload: dict):
        if not self.webhook_url:
            return
        
        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"}
            )
            if response.status_code != 204:
                log.error(f"Failed to send Discord notification: {response.status_code} - {response.text}")
        except Exception as e:
            log.error(f"Error sending Discord notification: {e}")

    def send_order_new(self, order_data: dict):
        """
        Triggered when a new Limit/Market order is placed.
        """
        symbol = order_data.get("symbol")
        side = order_data.get("side")
        order_type = order_data.get("orderType")
        price = order_data.get("price")
        qty = order_data.get("qty")
        tp = order_data.get("takeProfit", "")
        sl = order_data.get("stopLoss", "")
        
        color = 3066993 if side == "Buy" else 15158332 # Green or Red
        
        embed = {
            "title": f"📢 [掛單] 新增 {symbol} {side} 單",
            "color": color,
            "fields": [
                {"name": "類型", "value": order_type, "inline": True},
                {"name": "價格", "value": str(price), "inline": True},
                {"name": "數量", "value": str(qty), "inline": True},
                {"name": "止盈 (TP)", "value": str(tp) if tp else "無", "inline": True},
                {"name": "止損 (SL)", "value": str(sl) if sl else "無", "inline": True},
            ],
            "footer": {"text": f"Bybit Monitor • {datetime.now().strftime('%H:%M:%S')}"}
        }
        
        self._send({"embeds": [embed]})

    def send_order_filled(self, trade_data: dict):
        """
        Triggered when an order is filled (Execution).
        """
        symbol = trade_data.get("symbol")
        side = trade_data.get("side")
        price = trade_data.get("execPrice")
        qty = trade_data.get("execQty")
        
        color = 3066993 if side == "Buy" else 15158332
        
        embed = {
            "title": f"⚡ [成交] {symbol} {side} 已進場/加倉",
            "color": color,
            "fields": [
                {"name": "成交價格", "value": str(price), "inline": True},
                {"name": "成交數量", "value": str(qty), "inline": True},
            ],
            "footer": {"text": f"Bybit Monitor • {datetime.now().strftime('%H:%M:%S')}"}
        }
        
        self._send({"embeds": [embed]})

    def send_order_cancel(self, order_data: dict):
        """
        Triggered when an order is cancelled.
        """
        symbol = order_data.get("symbol")
        side = order_data.get("side")
        price = order_data.get("price")
        
        embed = {
            "title": f"🗑️ [撤單] {symbol} {side} 訂單已取消",
            "color": 9807270, # Grey
            "description": f"原掛單價格: {price}",
            "footer": {"text": f"Bybit Monitor • {datetime.now().strftime('%H:%M:%S')}"}
        }
        
        self._send({"embeds": [embed]})
        
    def send_position_update(self, pos_data: dict):
        """
        Sends snapshot of current position.
        """
        symbol = pos_data.get("symbol")
        side = pos_data.get("side")
        size = pos_data.get("size")
        entry_price = pos_data.get("avgPrice")
        unrealized_pnl = float(pos_data.get("unrealisedPnl", 0))
        
        if float(size) == 0:
            # Position closed
            return 
            
        emoji = "🟢" if unrealized_pnl >= 0 else "🔴"
        color = 3066993 if unrealized_pnl >= 0 else 15158332
        
        embed = {
            "title": f"📊 [持倉更新] {symbol} {side}",
            "color": color,
            "fields": [
                {"name": "持倉大小", "value": str(size), "inline": True},
                {"name": "入場均價", "value": str(entry_price), "inline": True},
                {"name": "未實現盈虧", "value": f"{emoji} {unrealized_pnl:.2f} U", "inline": False},
            ],
            "footer": {"text": f"Bybit Monitor • {datetime.now().strftime('%H:%M:%S')}"}
        }
        
        self._send({"embeds": [embed]})

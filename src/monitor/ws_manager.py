from pybit.unified_trading import WebSocket
from time import sleep
from .notifier import DiscordNotifier
from ..config import settings
from ..utils.logger import log

class BybitMonitor:
    def __init__(self):
        self.notifier = DiscordNotifier()
        self.ws = WebSocket(
            testnet=False,
            channel_type="private",
            api_key=settings["bybit_api_key"],
            api_secret=settings["bybit_api_secret"],
        )

    def _on_order_update(self, message):
        """
        Callback for order stream.
        """
        data = message.get("data", [])
        for order in data:
            status = order.get("orderStatus")
            
            # log.debug(f"Order Update: {order.get('symbol')} - {status}")
            
            if status == "New":
                self.notifier.send_order_new(order)
            elif status == "Cancelled":
                self.notifier.send_order_cancel(order)
            # We handle 'Filled' via execution stream for better detail, 
            # though 'Filled' order updates also come here.
            # To avoid double notification, we might ignore Filled here 
            # or rely on this one if execution stream is delayed.
            # Usually execution stream is preferred for fills.
            
    def _on_execution_update(self, message):
        """
        Callback for execution stream (trades).
        """
        data = message.get("data", [])
        for trade in data:
            # log.debug(f"Execution: {trade.get('symbol')} - {trade.get('execQty')} @ {trade.get('execPrice')}")
            self.notifier.send_order_filled(trade)

    def _on_position_update(self, message):
        """
        Callback for position stream.
        """
        data = message.get("data", [])
        for pos in data:
            # Filter out empty updates if needed, though Bybit usually sends relevant changes
            self.notifier.send_position_update(pos)

    def start(self):
        log.info("Connecting to Bybit Private WebSocket...")
        
        self.ws.order_stream(callback=self._on_order_update)
        self.ws.execution_stream(callback=self._on_execution_update)
        self.ws.position_stream(callback=self._on_position_update)
        
        log.info("Bybit Monitor started! Listening for events...")
        
        # Determine if we want to run a status loop here or just keep the script alive
        while True:
            sleep(60)

if __name__ == "__main__":
    monitor = BybitMonitor()
    monitor.start()

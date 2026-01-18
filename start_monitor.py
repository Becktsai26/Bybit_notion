from src.monitor.ws_manager import BybitMonitor
from src.utils.logger import log

if __name__ == "__main__":
    try:
        log.info("--- Starting Bybit Real-time Monitor ---")
        monitor = BybitMonitor()
        monitor.start()
    except KeyboardInterrupt:
        log.info("Monitor stopped by user.")
    except Exception as e:
        log.error(f"Monitor crashed: {e}")

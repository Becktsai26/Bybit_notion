# src/services/sync.py
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from ..adapters.base import BaseExchangeAdapter
from ..clients.notion import NotionClient
from ..utils.logger import log

class SyncService:
    """
    Orchestrates the synchronization process between an exchange and Notion.
    """

    def __init__(self, exchange_adapter: BaseExchangeAdapter, notion_client: NotionClient):
        self.exchange = exchange_adapter
        self.notion = notion_client

    def run_sync(self):
        """
        Runs the main synchronization logic with support for multi-window fetching.
        """
        log.info("Starting synchronization process...")
        
        # 1. Determine the time window
        # User requested backfill from 2026-01-01
        start_time_ms = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        log.info(f"Forcing start date to: {datetime.fromtimestamp(start_time_ms/1000, tz=timezone.utc)}")
        
        end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # 2. Skip subaccount notice for brevity
        log.warning("Note: Syncing main account only.")

        # 3. Fetch data from Bybit in 7-day chunks (API limit)
        all_transactions = []
        current_start = start_time_ms
        
        while current_start < end_time_ms:
            # 7 days max per request
            current_end = min(current_start + (7 * 24 * 60 * 60 * 1000) - 1, end_time_ms)
            
            log.info(f"Fetching chunk from {datetime.fromtimestamp(current_start/1000, tz=timezone.utc)} to {datetime.fromtimestamp(current_end/1000, tz=timezone.utc)}")
            
            try:
                chunk_txs = self.exchange.fetch_transaction_log(
                    account_type="UNIFIED", 
                    category="linear", 
                    start_time=int(current_start), 
                    end_time=int(current_end)
                )
                all_transactions.extend(chunk_txs)
            except Exception as e:
                log.error(f"Error fetching chunk: {e}")
                break
                
            current_start = current_end + 1

        log.info(f"Total transactions retrieved: {len(all_transactions)}")

        # 4. Process and transform data
        notion_records = []
        pnl_threshold = 0.5 # Filter out tiny amounts (often residuals or funding-like dust)
        
        for tx_record in all_transactions:
            if tx_record.get("type") != "TRADE":
                continue
                
            change = float(tx_record.get("change", 0.0))
            fee = float(tx_record.get("fee", 0.0))
            pnl = change + fee
            
            # Filter non-zero and above threshold
            if abs(pnl) < pnl_threshold:
                continue

            record = {
                "symbol": tx_record.get("symbol"),
                "side": tx_record.get("side"),
                "size": float(tx_record.get("qty", 0.0)),
                "price": float(tx_record.get("tradePrice", 0.0)),
                "fee": fee,
                "pnl": pnl,
                "timestamp": int(tx_record.get("transactionTime")),
                "subaccount": "Main Account"
            }
            notion_records.append(record)

        # Sort all records by timestamp
        notion_records.sort(key=lambda r: r['timestamp'])
        
        if not notion_records:
            log.info("No records matching the filter were found.")
            return

        log.info(f"Processed {len(notion_records)} records (PnL > {pnl_threshold}) to be written to Notion.")
        
        # 5. Write to Notion
        self.notion.create_records(notion_records)
        log.info("Synchronization process completed successfully.")

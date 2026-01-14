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
        Runs the main synchronization logic.
        - Determines the time window for fetching data.
        - Fetches data for the main account and all subaccounts.
        - Processes and transforms the data.
        - Writes the new data to Notion.
        """
        log.info("Starting synchronization process...")
        
        # 1. Determine the time window
        last_sync_ms = self.notion.get_last_sync_timestamp()
        if last_sync_ms:
            # Start from the second after the last sync to avoid duplicates
            start_time_ms = last_sync_ms + 1
        else:
            # If DB is empty, fetch last 7 days (Bybit's limit for execution records)
            start_time_ms = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)
            log.info("No previous sync found. Fetching data for the last 7 days.")
        
        end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # 2. Get accounts to sync (main + subaccounts)
        # The main account is handled by the default adapter instance.
        # We need to create new adapter instances for subaccounts if credentials differ,
        # but Bybit's API allows querying subaccount data with a Master Key.
        # For this implementation, we assume a Master Key with subaccount permissions.
        
        # For now, let's simplify and only use the main account provided.
        # Sub-account iteration logic can be complex due to credential management.
        # We will log a placeholder for this functionality.
        log.warning("Note: Subaccount iteration is not fully implemented in this version. Syncing main account only.")

        # 3. Fetch data from Bybit
        log.info(f"Fetching data from {datetime.fromtimestamp(start_time_ms/1000)} to {datetime.fromtimestamp(end_time_ms/1000)}")
        
        # Fetch executions (trades)
        executions = self.exchange.fetch_executions(category="linear", start_time=start_time_ms, end_time=end_time_ms)
        
        # Fetch transaction log (for funding fees, etc.)
        transactions = self.exchange.fetch_transaction_log(
            account_type="UNIFIED", 
            category="linear", 
            start_time=start_time_ms, 
            end_time=end_time_ms
        )

        log.info(f"Found {len(executions)} new executions and {len(transactions)} new transactions.")

        # 4. Process and transform data
        notion_records = []
        
        # Process executions
        for exec_record in executions:
            # Filter out records that are somehow older than our start time
            exec_time_ms = int(exec_record.get("execTime"))
            if exec_time_ms < start_time_ms:
                continue

            record = {
                "symbol": exec_record.get("symbol"),
                "side": exec_record.get("side"),
                "size": float(exec_record.get("execQty")),
                "price": float(exec_record.get("execPrice")),
                "fee": float(exec_record.get("execFee")),
                "pnl": float(exec_record.get("closedPnl", 0.0)),
                "timestamp": exec_time_ms,
                "subaccount": "Main Account" # Placeholder
            }
            notion_records.append(record)

        # Process funding fees from transaction log
        for tx_record in transactions:
            tx_time_ms = int(tx_record.get("transactionTime"))
            if tx_time_ms < start_time_ms:
                continue
            
            if tx_record.get("type") == "FUNDING":
                record = {
                    "symbol": tx_record.get("symbol"),
                    "side": "Funding", # Special side for funding
                    "size": None,
                    "price": None,
                    "fee": float(tx_record.get("change")), # Funding is treated as a fee/gain
                    "pnl": float(tx_record.get("change")),
                    "timestamp": tx_time_ms,
                    "subaccount": "Main Account" # Placeholder
                }
                notion_records.append(record)

        # Sort all records by timestamp before writing
        notion_records.sort(key=lambda r: r['timestamp'])
        
        if not notion_records:
            log.info("No new records to sync.")
            return

        log.info(f"Processed {len(notion_records)} new records to be written to Notion.")
        
        # 5. Write to Notion
        self.notion.create_records(notion_records)

        log.info("Synchronization process completed successfully.")


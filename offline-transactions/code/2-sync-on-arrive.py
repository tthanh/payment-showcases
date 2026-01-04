"""Handles synchronization after reconnecting"""
from datetime import datetime, timedelta
from typing import List

from models import Transaction, TransactionStatus, TerminalConfig

class SyncService:
    """Syncs offline transactions back to the bank"""
    
    def __init__(self, config: TerminalConfig):
        self.config = config
    
    def sync_transactions(self, transactions: List[Transaction], bank) -> dict:
        results = {"synced": 0, "expired": 0}
        
        for txn in transactions:
            # TTL: check expiration
            age = datetime.now() - txn.timestamp
            if age > timedelta(hours=self.config.ttl_hours):
                txn.status = TransactionStatus.DECLINED
                results["expired"] += 1
                continue
            
            # Send to bank
            txn.status = TransactionStatus.SYNCED
            results["synced"] += 1
        
        # Submit batch to bank
        bank_results = bank.settle_batch([t for t in transactions if t.status == TransactionStatus.SYNCED])
        results.update(bank_results)
        
        return results
"""Acquirer bank - receives and processes synced transactions"""
from typing import List

from models import Transaction


class AcquirerBank:
    """Airline's bank - handles batch settlements"""
    
    def __init__(self, name: str):
        self.name = name
        self.settled_transactions: List[Transaction] = []
        self.processed_ids: set[str] = set()  # Critical: prevents double-charging
    
    def settle_batch(self, transactions: List[Transaction]) -> dict:
        """
        Process a batch of transactions from terminal.
        Idempotent: safe to retry without duplicate charges.
        """
        results = {"settled": 0, "duplicates": 0, "rejected": 0}
        
        for txn in transactions:
            # Idempotency check: already processed?
            if txn.id in self.processed_ids:
                results["duplicates"] += 1
                continue
            
            # Validation
            if txn.amount <= 0:
                results["rejected"] += 1
                continue
            
            # Process once and only once
            self.processed_ids.add(txn.id)
            self.settled_transactions.append(txn)
            results["settled"] += 1
        
        return results
    
    def issue_compensating_transaction(self, transaction: Transaction):
        """
        Handle declined transactions after settlement.
        In reality, this pushes customer account into negative balance.
        """
        pass  # Implementation depends on issuer bank integration
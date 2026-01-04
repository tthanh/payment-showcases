"""Edge node - makes authorization decisions locally"""
import uuid
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List

from models import Card, Transaction, TransactionStatus, TerminalConfig


class EdgeTerminal:
    """Payment terminal operating at the edge (offline)"""
    
    def __init__(self, config: TerminalConfig):
        self.config = config
        self.storage: List[Transaction] = []
        self.velocity_tracker: dict[str, List[datetime]] = defaultdict(list)
    
    def authorize(self, card: Card, amount: float, seat: str) -> Transaction:
        """Make authorization decision at the edge"""
        
        transaction = Transaction(
            id=str(uuid.uuid4()),
            card_number=card.number,
            amount=amount,
            timestamp=datetime.now(),
            seat=seat,
            status=TransactionStatus.PENDING_SYNC
        )
        
        # Risk checks (order matters)
        if self._is_blacklisted(card.number):
            transaction.status = TransactionStatus.DECLINED
            return transaction
        
        if not self._check_velocity(card.number):
            transaction.status = TransactionStatus.DECLINED
            return transaction
        
        if not card.can_transact_offline():
            transaction.status = TransactionStatus.DECLINED
            return transaction
        
        if amount > self.config.floor_limit:
            transaction.status = TransactionStatus.DECLINED
            return transaction
        
        # Approve and store
        transaction.status = TransactionStatus.APPROVED
        card.increment_counter()
        self.storage.append(transaction)
        
        return transaction
    
    def _is_blacklisted(self, card_number: str) -> bool:
        """Check stolen/blocked cards"""
        """The blacklist is downloaded periodically from central system"""
        return card_number in self.config.blacklist
    
    def _check_velocity(self, card_number: str) -> bool:
        """Detect suspicious transaction patterns"""
        now = datetime.now()
        
        # Remove old entries (>2 minutes)
        self.velocity_tracker[card_number] = [
            t for t in self.velocity_tracker[card_number]
            if (now - t).seconds < 120
        ]
        
        if len(self.velocity_tracker[card_number]) >= self.config.velocity_limit:
            return False
        
        self.velocity_tracker[card_number].append(now)
        return True
    
    def get_pending_transactions(self) -> List[Transaction]:
        """Get transactions ready for sync"""
        return [t for t in self.storage if t.status == TransactionStatus.PENDING_SYNC]
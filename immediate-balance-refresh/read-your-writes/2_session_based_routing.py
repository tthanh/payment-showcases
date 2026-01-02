import time
from dataclasses import dataclass
from typing import Dict, Optional
from datetime import datetime, timedelta

@dataclass
class Account:
    user_id: str
    balance: float
    version: int = 0

@dataclass
class UserSession:
    user_id: str
    last_write_time: Optional[datetime] = None
    last_write_version: Optional[int] = None

leader_db: Dict[str, Account] = {}
follower_db: Dict[str, Account] = {}
user_sessions: Dict[str, UserSession] = {}

SESSION_STICKINESS_SECONDS = 5

def make_payment(user_id: str, payment_amount: float) -> tuple[float, int]:
    """
    Track the last write timestamp for each user session.
    Updates session to enable smart routing for subsequent reads.
    """
    account = leader_db[user_id]
    account.balance -= payment_amount
    account.version += 1
    
    # Update session with write timestamp
    session = user_sessions[user_id]
    session.last_write_time = datetime.now()
    session.last_write_version = account.version
    
    return account.balance, account.version

def read_balance(user_id: str) -> float:
    """
    If a read request comes within X seconds of a write, route it to Leader.
    Otherwise, route to Follower for load balancing.
    """
    session = user_sessions[user_id]
    
    if session.last_write_time:
        time_since_write = (datetime.now() - session.last_write_time).total_seconds()
        if time_since_write < SESSION_STICKINESS_SECONDS:
            # Recent write detected - route to Leader for consistency
            return leader_db[user_id].balance
    
    # No recent write - route to Follower for load balancing
    return follower_db[user_id].balance



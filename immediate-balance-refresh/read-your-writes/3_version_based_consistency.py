from dataclasses import dataclass
from typing import Dict, Optional
from datetime import datetime

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

def make_payment(user_id: str, payment_amount: float) -> tuple[float, int]:
    """
    Tag each write with a monotonically increasing version number.
    Store the version in user session for subsequent read requests.
    """
    account = leader_db[user_id]
    account.balance -= payment_amount
    account.version += 1
    
    # Store version in session for read consistency checks
    session = user_sessions[user_id]
    session.last_write_time = datetime.now()
    session.last_write_version = account.version
    
    return account.balance, account.version

def read_balance(user_id: str) -> float:
    """
    On read, check if Follower has caught up to the required version.
    If not, either wait briefly or route to Leader.
    """
    session = user_sessions[user_id]
    required_version = session.last_write_version
    
    if required_version is None:
        # No writes yet - can safely use Follower
        return follower_db[user_id].balance
    
    follower_account = follower_db[user_id]
    
    if follower_account.version >= required_version:
        # Follower has caught up - use it for load balancing
        return follower_account.balance
    else:
        # Follower is behind - route to Leader for consistency
        return leader_db[user_id].balance



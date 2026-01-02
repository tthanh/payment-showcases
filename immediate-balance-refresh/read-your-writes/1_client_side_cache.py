from dataclasses import dataclass
from typing import Dict

@dataclass
class Account:
    user_id: str
    balance: float
    version: int = 0

class PaymentAPIResponse:
    def __init__(self, success: bool, new_balance: float, version: int):
        self.success = success
        self.new_balance = new_balance
        self.version = version

leader_db: Dict[str, Account] = {}
follower_db: Dict[str, Account] = {}

def make_payment(user_id: str, payment_amount: float) -> PaymentAPIResponse:
    """
    When payment succeeds, return the updated balance in the API response.
    Cache it on the client side and display it immediately without another database query.
    """
    account = leader_db[user_id]
    account.balance -= payment_amount
    account.version += 1
    
    # Return the new balance in API response for immediate client display
    return PaymentAPIResponse(
        success=True, 
        new_balance=account.balance, 
        version=account.version
    )

def display_balance_from_cache(cached_response: PaymentAPIResponse) -> float:
    """
    Use the balance from the API response cache instead of querying database.
    This provides instant UI updates.
    """
    return cached_response.new_balance



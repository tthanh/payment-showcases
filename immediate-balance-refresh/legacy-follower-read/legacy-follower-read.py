import time
import random
import logging
from dataclasses import dataclass
from typing import Dict

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s: %(message)s', 
                    datefmt='%H:%M:%S')
logger_user = logging.getLogger("USER-ACTION")
logger_leader = logging.getLogger("LEADER-DB")
logger_follower = logging.getLogger("FOLLOWER-DB")
logger_app = logging.getLogger("APP-LOGIC")

@dataclass
class Account:
    user_id: str
    balance: float
    version: int = 0  # Track version for demonstration purposes

# Mock Database: Leader and Followers
leader_db: Dict[str, Account] = {}
follower_db_1: Dict[str, Account] = {}
follower_db_2: Dict[str, Account] = {}

# Track replication lag (simulated)
REPLICATION_LAG_MS = random.randint(100, 500)  # Typical replication lag

def initialize_user(user_id: str, initial_balance: float):
    """Initialize a user account in all databases."""
    account = Account(user_id=user_id, balance=initial_balance, version=1)
    
    # Write to leader
    leader_db[user_id] = account
    
    # Replicate to followers (simulating they're already in sync initially)
    follower_db_1[user_id] = Account(user_id=user_id, balance=initial_balance, version=1)
    follower_db_2[user_id] = Account(user_id=user_id, balance=initial_balance, version=1)
    
    logger_leader.info(f"Initialized {user_id} with balance ${initial_balance}")

def write_payment(user_id: str, payment_amount: float):
    """
    Write payment to Leader DB (simulates payment processing).
    Returns the new balance.
    """
    logger_user.info(f"🔵 User clicks 'Pay ${payment_amount}' button")
    
    # Write always goes to Leader
    account = leader_db[user_id]
    old_balance = account.balance
    account.balance -= payment_amount
    account.version += 1
    
    logger_leader.info(f"✅ WRITE completed: {user_id} balance ${old_balance} → ${account.balance} (v{account.version})")
    
    # Simulate async replication to followers (happens in background)
    # In reality, this is handled by the database replication mechanism
    return account.balance

def replicate_to_followers():
    """
    Simulate asynchronous replication from Leader to Followers.
    This happens with a lag (100-500ms in our simulation).
    """
    time.sleep(REPLICATION_LAG_MS / 1000.0)  # Convert ms to seconds
    
    for user_id, leader_account in leader_db.items():
        follower_db_1[user_id] = Account(
            user_id=leader_account.user_id,
            balance=leader_account.balance,
            version=leader_account.version
        )
        follower_db_2[user_id] = Account(
            user_id=leader_account.user_id,
            balance=leader_account.balance,
            version=leader_account.version
        )
    
    logger_follower.info(f"⚡ Replication completed (after {REPLICATION_LAG_MS}ms lag)")

def read_balance_from_follower(user_id: str) -> float:
    """
    Read balance from a random Follower (to distribute read load).
    This is the PROBLEM: might return stale data!
    """
    # Randomly pick a follower
    follower = random.choice([follower_db_1, follower_db_2])
    follower_name = "Follower-1" if follower is follower_db_1 else "Follower-2"
    
    account = follower[user_id]
    logger_follower.info(f"📖 READ from {follower_name}: {user_id} balance ${account.balance} (v{account.version})")
    
    return account.balance

def user_journey():
    """
    Simulates a typical user journey that exposes the Read-after-Write problem.
    """
    user_id = "user_alice"
    payment_amount = 100.0
    
    print("\n" + "="*80)
    print("SCENARIO: User makes a payment and immediately views their dashboard")
    print("="*80 + "\n")
    
    # Step 1: Initialize user with $1000 balance
    initialize_user(user_id, 1000.0)
    time.sleep(0.5)  # Brief pause for readability
    
    # Step 2: User makes a payment
    new_balance = write_payment(user_id, payment_amount)
    logger_app.info(f"💳 Payment API returned: balance should be ${new_balance}")
    
    # Step 3: App immediately redirects to dashboard
    logger_user.info(f"🔄 Browser redirects to 'Account Summary' page...")
    time.sleep(0.05)  # User redirect takes ~50ms
    
    # Step 4: Dashboard reads from Follower (for load balancing)
    # PROBLEM: Replication hasn't completed yet!
    displayed_balance = read_balance_from_follower(user_id)
    
    print("\n" + "-"*80)
    logger_app.info(f"🖥️  Dashboard displays: ${displayed_balance}")
    
    # Check if there's a consistency problem
    if displayed_balance != new_balance:
        logger_app.error(f"❌ CONSISTENCY PROBLEM DETECTED!")
        logger_app.error(f"   Expected: ${new_balance}, but showing: ${displayed_balance}")
        logger_app.error(f"   User thinks payment failed and might pay again! 💥")
    else:
        logger_app.info(f"✅ Balance is correct (got lucky with timing)")
    
    print("-"*80 + "\n")
    
    # Step 5: Simulate replication catching up
    logger_leader.info("⏳ Waiting for replication to catch up...")
    replicate_to_followers()
    
    # Step 6: User refreshes page after replication completes
    time.sleep(0.5)
    logger_user.info(f"🔄 User manually refreshes the page...")
    correct_balance = read_balance_from_follower(user_id)
    logger_app.info(f"🖥️  Dashboard now displays: ${correct_balance}")
    
    if correct_balance == new_balance:
        logger_app.info(f"✅ Now showing correct balance (but damage may be done)")
    
    print("\n" + "="*80)
    print("SUMMARY:")
    print(f"  • Payment was successful: ${1000} → ${new_balance}")
    print(f"  • But user initially saw: ${displayed_balance} (STALE DATA)")
    print(f"  • Replication lag: {REPLICATION_LAG_MS}ms")
    print(f"  • Risk: User might submit duplicate payment!")
    print("="*80 + "\n")

if __name__ == "__main__":
    # Run the demonstration
    user_journey()
    
    print("\n💡 This demonstrates the classic 'Read-after-Write' consistency problem.")
    print("   See '../read-your-writes/read-your-writes.py' for solutions.\n")

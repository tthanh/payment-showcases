import uuid
import random
import logging
from dataclasses import dataclass

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(name)s: %(message)s')
logger = logging.getLogger("BATCH-PAYRUN")

@dataclass
class Payment:
    id: str
    amount: float
    status: str  # PENDING, PROCESSING, COMPLETED

# Mock Database Store (Expected to contain data in a real scenario)
database = {"payments": {}}

def run_legacy_batch():
    """
    Simulates a periodic SQL job.
    Logic: Poll for all pending rows, then iterate through them.
    """
    logger.info(">>> JOB START: Querying database for PENDING payments...")
    
    # 1. Fetching all matching records at once (Batch Selection)
    # This simulates: SELECT id FROM payments WHERE status = 'PENDING'
    pending_ids = [p_id for p_id, p in database["payments"].items() if p.status == "PENDING"]
    
    if not pending_ids:
        logger.info("No work found. Job exiting.")
        return

    logger.info(f"Found {len(pending_ids)} payments. Beginning loop...")
    
    # 2. Sequential processing
    for p_id in pending_ids:
        payment = database["payments"][p_id]
        
        # Update state locally
        payment.status = "PROCESSING"
        
        # Process logic (e.g. external API call would go here)
        payment.status = "COMPLETED"
        
        logger.info(f"DONE: Payment {p_id} updated to COMPLETED.")

    logger.info(">>> JOB FINISHED.")

if __name__ == "__main__":
    # In this version, we assume the 'database' is populated by an external process (API)
    # before this script is executed.
    run_legacy_batch()
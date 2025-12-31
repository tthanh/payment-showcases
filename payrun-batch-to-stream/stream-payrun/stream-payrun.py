import time
import uuid
import logging
import threading
from dataclasses import dataclass, field
from queue import Queue

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(name)s: %(message)s')
logger_app = logging.getLogger("APP-LOGIC")
logger_relay = logging.getLogger("RELAY-PROCESS")
logger_worker = logging.getLogger("STREAM-WORKER")

@dataclass
class Payment:
    id: str
    amount: float
    status: str

@dataclass
class OutboxEvent:
    id: str
    payment_id: str
    processed: bool = False

# Mock Database Store
database = {
    "payments": {},
    "outbox": {}
}

# The Message Stream (Simulating Kafka or RabbitMQ)
message_stream = Queue()

# The API, the Relay Process, and the Stream Worker run in separate processes/threads

# The API
def create_payment(amount: float):
    """
    Simulates the 'All-in-One Save' (Transactional Outbox Pattern).
    Saves both the payment data and the event to the database atomically.
    """
    payment_id = f"pay_{uuid.uuid4().hex[:6]}"
    event_id = f"evt_{uuid.uuid4().hex[:6]}"
    
    # In a real DB, these two lines happen inside a single BEGIN/COMMIT block
    database["payments"][payment_id] = Payment(id=payment_id, amount=amount, status="PENDING")
    database["outbox"][event_id] = OutboxEvent(id=event_id, payment_id=payment_id)
    
    logger_app.info(f"Created {payment_id} and saved Event {event_id} to Outbox.")

# The Relay Process
def relay_process():
    """
    Simulates the 'Instant Hand-off'.
    Watches the Outbox table and pushes events to the Message Stream.
    """
    logger_relay.info("Relay started. Watching Outbox table...")
    while True:
        # Check for unprocessed events in the outbox
        unprocessed = [e for e in database["outbox"].values() if not e.processed]
        
        for event in unprocessed:
            # Push to the "Stream"
            message_stream.put(event.payment_id)
            # Mark as processed in the Outbox
            event.processed = True
            logger_relay.info(f"Relayed event for {event.payment_id} to stream.")
            
        time.sleep(1) # Polling the outbox frequently

# The Stream Worker
def stream_worker():
    """
    Simulates 'One Task at a Time'.
    Listens to the stream and processes each payment individually.
    """
    logger_worker.info("Worker started. Waiting for events...")
    while True:
        payment_id = message_stream.get() # Blocks until an event arrives
        
        # Process the individual payment
        payment = database["payments"][payment_id]
        payment.status = "COMPLETED"

        # 1. Fetch current payment state
        payment = database["payments"][payment_id]
        
        # 2. Idempotency Check: Don't process if already done
        # This prevents the "Double-Charging Risk" if the worker retries an event
        if payment.status == "COMPLETED":
            logger_worker.warning(f"SKIPPING: {payment_id} is already COMPLETED. (Idempotency Guard)")
            message_stream.task_done()
            continue
            
        # 3. Simulated Bank API Call
        logger_worker.info(f"START: Calling Bank API for {payment_id}...")
        
        logger_worker.info(f"DONE: Processed {payment_id} instantly. Status: {payment.status}")
        message_stream.task_done()
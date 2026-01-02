## Purpose
To show how Edge Computing handles payments without internet, balancing the need for fast sales with the bank's need for security.

## The Problem

Imagine you’re on a plane with no Wi-Fi. You buy a $10 snack, tap your card, and it says "Approved" instantly.

But there’s no internet. No connection to the bank. So how did your payment get approved so fast? Who said "yes"?

![alt text](image.png)

Let’s see who is involved in this scenario and what they want. In this system, every player has their own goal and their own risk.

| Actor | Motivation (The Need) | Ideal Outcome (The Win) | Risks (The Lose) |
| :--- | :--- | :--- | :--- |
| **Customer** | **Pay anytime:** Wants to pay and eat without waiting for a "Connecting" screen. | **Fast approval:** The "Approved" message shows up in less than 1 second. | **Surprise fees:** Finding an unexpected bank fee later because the charge hit late. |
| **Merchant** (Airline) | **Making more sales:** Needs to serve 100 people quickly in a short flight. | **No lost sales:** Every tap works; no food or snacks go to waste. | **Losing money:** Giving away food to a card that is actually declined later. |
| **Acquirer Bank** (Airline Bank) | **Keeping the client:** Wants to provide tech that works so the airline stays with them. | **Easy data sync:** Successfully moving thousands of sales to the bank after landing. | **Software errors:** If the system loses a "batch" of sales, the bank might owe the airline money. |
| **Issuer Bank** (Your Bank) | **Keeping records safe:** Wants to be the main source of truth for your money. | **Earning fees:** Accepting the offline payment and collecting a small fee from the user. | **Stolen cards:** A stolen card being used on many flights before it can be blocked. |
| **Payment Network** (Visa/MC) | **Trust everywhere:** Wants a card from one country to work on a flight in another. | **Fair rules:** Creating clear rules on who pays if a transaction fails. | **Loss of trust:** If offline risk is too high, people might stop trusting cards for travel. |
| **Terminal Provider** | **Strong hardware:** Needs the device to be tough and reliable for flight crews. | **Saving all data:** The device safely stores 100% of the sales in its own memory. | **Losing data:** A battery failure or bug that wipes out the saved sales info. |

## The Challenges & Trade-offs

When a terminal goes offline, it cannot talk to the central bank. This creates two main risks:

* **The Double Spend Risk:** A user might have $0 in their actual account, but the offline terminal doesn't know that.
* **The Sync Conflict**: A passenger spends the same $10 on two different food carts. Since the carts don't talk to each other, the bank only catches the "double-spend" after the plane lands.

In this case, we choose **Availability** (making the payment now) over **Consistency** (verifying the bank balance). The business logic is simple: the benefit of accepting the payment immediately outweighs the risk of a late decline.

## Strategy: Edge Computing

In a normal store, a terminal is just a "messenger." It sends your card info to the bank in the cloud and waits for a "Yes" or "No." This is **Cloud Computing**.

But on a plane, the terminal is cut off. To solve this, we move the compute - the decision-making "brain" - closer to the user (the edge). The terminal becomes an Edge Node. Instead of waiting for the cloud, it authorizes your card locally and make decisions right in the aisle. This is **Edge Computing**.

## The solution

### 1. Before Take-off (Rules & Data)

While the terminal still has a fast internet connection, it prepares for offline mode:
- **Floor Limits:** Sets a "safe" price (like $20) so anything under that is auto-approved for speed.
- **Blacklist (Risk Data):** Downloads a list of stolen or blocked cards to instantly reject "bad" cards, even without the bank.
- **Passenger Metadata:** Syncs with the flight manifest, tagging each sale with seat and name for follow-up if a payment fails.

### 2. During the Flight (The Edge)

Once offline, the terminal acts as an independent Edge Node:
- **Local Storage (Store and Forward):** Saves every sale in secure memory-this is the temporary source of truth.
- **Velocity Checks:** Watches for suspicious patterns (like 5 taps in 2 minutes) and blocks cards locally if needed.
- **Cryptographic Verification:** Checks the card’s digital signature to ensure it’s an authentic card, not a fake.
- **Offline Chip Counters:** By EMV standards, the card chip stores **Offline Accumulators**, making it physically reject sales once "Lower" or "Upper" limits are hit until it reconnects to a bank.

### 3. When Arriving (Sync)

After landing and reconnecting, the terminal syncs all sales back to the bank:
- **Idempotency:** Each sale has a unique key so even if data is sent twice, the customer is only charged once.
- **Compensating Transactions:** The Issuer Bank (customer's bank) honors the airline's payment first, then recovers the debt by pushing the customer’s account into a negative balance.
- **TTL (Time-to-Live):** The Network and Issuer set a 24–48 hour window; if the Acquirer (airline's bank) submits the data late, the airline loses the right to collect the funds.
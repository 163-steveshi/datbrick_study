import csv
import random
from datetime import datetime
from pathlib import Path
from faker import Faker

# 1. Deterministic Seeding for reproducible data generation
SEED_VALUE = 42
random.seed(SEED_VALUE)
Faker.seed(SEED_VALUE)

fake = Faker()

# Configuration
NUM_TRANSACTIONS = 20000

# Using pathlib to safely construct the path under data/trans/ regardless of OS
OUTPUT_DIR = Path("data") / "trans"
OUTPUT_DIR.mkdir(
    parents=True, exist_ok=True
)  # Automatically creates data/trans folders if missing
OUTPUT_FILE = OUTPUT_DIR / f"transaction_{NUM_TRANSACTIONS}.csv"

# Setup global categories for consistent distribution
PAYMENT_TYPES = ["Credit Card", "Debit Card", "Mobile Wallet", "Cash"]
COUNTRIES = ["US", "CA", "HK", "UK", "SG"]
CHANNELS = ["Google Ads", "Organic Search", "Email", "Social Media", "Affiliate"]

# Base profile configurations combining transaction patterns with marketing channels
PROFILES = {
    "low_high": {
        "amount_range": (2.00, 15.00),
        "countries": ["CA", "HK"],
        "payments": ["Debit Card", "Mobile Wallet"],
        "channels": ["Social Media", "Affiliate"],
    },
    "high_low": {
        "amount_range": (150.00, 800.00),
        "countries": ["US", "UK"],
        "payments": ["Credit Card"],
        "channels": ["Google Ads", "Organic Search"],
    },
    "avg_avg": {
        "amount_range": (15.00, 80.00),
        "countries": ["US", "CA", "SG"],
        "payments": ["Credit Card", "Debit Card", "Cash"],
        "channels": ["Email", "Organic Search"],
    },
}

headers = [
    "transaction_id",
    "item_sequence",
    "customer_id",
    "timestamp",
    "item_unit_price",
    "item_quantity",
    "payment_type",
    "country",
    "channel",
    "hour_of_day",
    "is_weekend",
    "is_noise_row",
]

print(
    f"Generating deterministic POS transaction data with channels and ~2% noise using seed {SEED_VALUE}..."
)

with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(headers)

    for _ in range(NUM_TRANSACTIONS):
        # Select a baseline profile archetype
        archetype = random.choice(["low_high", "high_low", "avg_avg"])
        cfg = PROFILES[archetype]

        tx_id = f"TX_{fake.unique.random_number(digits=8)}"
        cust_id = f"CUST_{random.randint(1000, 1999)}"

        # Generate transaction level timestamp
        random_date = fake.date_time_between(start_date="-1y", end_date="now")
        timestamp_str = random_date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        hour_of_day = random_date.hour
        is_weekend = 1 if random_date.weekday() >= 5 else 0

        # Base item rules
        num_items = (
            random.randint(1, 2) if archetype == "high_low" else random.randint(1, 6)
        )

        for seq in range(1, num_items + 1):
            # Determine if this specific row becomes a noise/outlier data point (~2% chance)
            is_noise = 1 if random.random() < 0.02 else 0

            if is_noise:
                # Inject noise: Break profile correlations completely
                price = (
                    round(random.uniform(5000.00, 25000.00), 2)
                    if random.choice([True, False])
                    else 0.01
                )
                quantity = (
                    random.randint(50, 100) if price < 10 else random.randint(1, 2)
                )
                country = random.choice(COUNTRIES)
                payment_type = random.choice(PAYMENT_TYPES)
                channel = random.choice(CHANNELS)
            else:
                # Standard profile-driven generation
                price = round(random.uniform(*cfg["amount_range"]), 2)
                quantity = (
                    random.randint(1, 2)
                    if archetype == "high_low"
                    else random.randint(1, 5)
                )
                country = random.choice(cfg["countries"])
                payment_type = random.choice(cfg["payments"])
                channel = random.choice(cfg["channels"])

            writer.writerow(
                [
                    tx_id,
                    seq,
                    cust_id,
                    timestamp_str,
                    price,
                    quantity,
                    payment_type,
                    country,
                    channel,
                    hour_of_day,
                    is_weekend,
                    is_noise,
                ]
            )

print(f"Success! Data with marketing channel tracking saved to '{OUTPUT_FILE}'")

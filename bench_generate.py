"""Generate large test datasets for Blitz distance testing."""
import json
import random
import sys
import time

def generate(n: int, path: str):
    start = time.time()
    data = []
    categories = ["electronics", "clothing", "food", "tools", "books", "sports", "auto", "home"]
    cities = ["NYC", "LA", "Chicago", "Houston", "Phoenix", "Philly", "San Antonio", "Dallas"]

    for i in range(n):
        data.append({
            "id": i,
            "name": f"item_{i}",
            "price": round(random.uniform(1.0, 999.99), 2),
            "quantity": random.randint(1, 500),
            "category": random.choice(categories),
            "city": random.choice(cities),
            "rating": round(random.uniform(1.0, 5.0), 1),
            "in_stock": random.choice([True, False]),
            "weight_kg": round(random.uniform(0.1, 50.0), 2),
        })

    with open(path, "w") as f:
        json.dump(data, f)

    elapsed = time.time() - start
    size_mb = len(json.dumps(data)) / (1024 * 1024)
    print(f"Generated {n:,} rows ({size_mb:.1f}MB) in {elapsed:.1f}s -> {path}")

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    path = sys.argv[2] if len(sys.argv) > 2 else "bench_data.json"
    generate(n, path)

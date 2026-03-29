"""Smoke test: verifies all services are up and responding."""

import sys
import time
import requests


BASE_URL = "http://localhost:8080"
METRICS_ENDPOINTS = [
    ("ingest", "http://localhost:8001"),
    ("inference", "http://localhost:8002"),
    ("aggregator", "http://localhost:8080/metrics"),
]


def wait_for_health(url: str, timeout: int = 60) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{url}/health", timeout=3)
            if r.status_code == 200:
                return True
        except requests.ConnectionError:
            pass
        time.sleep(2)
    return False


def check_metrics(name: str, url: str) -> bool:
    try:
        r = requests.get(url, timeout=5)
        return r.status_code == 200 and "# HELP" in r.text
    except Exception as e:
        print(f"  FAIL {name} metrics ({url}): {e}")
        return False


def main():
    results = []

    # 1. Health check
    print("Checking aggregator health...")
    ok = wait_for_health(BASE_URL)
    results.append(("Aggregator health", ok))
    print(f"  {'PASS' if ok else 'FAIL'}")

    # 2. Streams endpoint
    print("Checking /api/streams...")
    try:
        r = requests.get(f"{BASE_URL}/api/streams", timeout=5)
        ok = r.status_code == 200 and "streams" in r.json()
        results.append(("Streams API", ok))
        print(f"  {'PASS' if ok else 'FAIL'} - {r.json()}")
    except Exception as e:
        results.append(("Streams API", False))
        print(f"  FAIL - {e}")

    # 3. Metrics endpoints
    for name, url in METRICS_ENDPOINTS:
        print(f"Checking {name} metrics...")
        ok = check_metrics(name, url)
        results.append((f"{name} metrics", ok))
        print(f"  {'PASS' if ok else 'FAIL'}")

    # Summary
    print("\n--- Results ---")
    all_pass = True
    for name, ok in results:
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name}")
        if not ok:
            all_pass = False

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()

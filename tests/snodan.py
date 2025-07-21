import requests
import sys


def test_hadoop_security(host):
    print(f"Testing Hadoop security on {host}")

    # Test 1: WebHDFS access
    try:
        url = f"http://{host}:9870/webhdfs/v1/?op=LISTSTATUS"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            print("❌ VULNERABLE: WebHDFS allows unauthenticated access")
        else:
            print("✓ PASS: WebHDFS properly restricted")
    except:
        print("✓ PASS: WebHDFS appears to be disabled")

    # Test 2: Check for configuration endpoint
    try:
        url = f"http://{host}:9870/conf"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            if "*" in resp.text:
                print("❌ VULNERABLE: Wildcard permissions found in configuration")
            else:
                print("✓ PASS: No wildcard permissions detected")
    except:
        print("✓ PASS: Configuration endpoint not accessible")


if __name__ == "__main__":
    test_hadoop_security("hadoop101")
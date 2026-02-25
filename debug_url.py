import requests
import datetime

url = "https://bcbsil.mrf.bcbs.com/2025-10_320_33B0_in-network-rates_12_of_31.json.gz?&Expires=1763906632&Signature=P4QaBrnndgUsOWBzyjAexkIx1BL1dK--JUBi0yRimACgxMks~mCB-lKFK6jEdhD~pAJd-9ZaIYwmrPMdMYfTPvyviXIOxRox2ycYZwejrA9UuEUiW4LI30A936xs84~7OPlktaAABpPQI8M6ilmESHLHbEIAfmxwc9kFXzugSBQivWab9mL6GKUrHpTqGWs6US1ccfeTAlFSs2h31TbUG6VgbDKdOazE7hytwn1zRBCOfDKZpTLshHAlqWzbUKojqtDIIFwN6lGr-HRvhbf2yyTAHS7I9KgQzUkRQ~HyLaVow967ySeCZ-vpfz-JKJ1xXJRyuhM8lRy1pyNYj8GoMA__&Key-Pair-Id=K27TQMT39R1C8A"

# Check expiration
expires_timestamp = 1763906632
expires_date = datetime.datetime.fromtimestamp(expires_timestamp)
current_date = datetime.datetime.now()

print(f"Expires Timestamp: {expires_timestamp}")
print(f"Expires Date: {expires_date}")
print(f"Current Date: {current_date}")

if current_date > expires_date:
    print("STATUS: EXPIRED")
else:
    print("STATUS: VALID")

# Try to fetch
print(f"\nFetching URL: {url[:100]}...")
try:
    response = requests.get(url, timeout=10)
    print(f"Status Code: {response.status_code}")
    print(f"Headers: {response.headers}")
    print(f"Content (first 100 bytes): {response.content[:100]}")
except Exception as e:
    print(f"Error fetching: {e}")

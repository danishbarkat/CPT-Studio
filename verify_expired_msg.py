import requests
import json

url = "http://localhost:5001/fetch_pricing"
expired_url = "https://bcbsil.mrf.bcbs.com/2025-10_320_33B0_in-network-rates_12_of_31.json.gz?&Expires=1763906632&Signature=P4QaBrnndgUsOWBzyjAexkIx1BL1dK--JUBi0yRimACgxMks~mCB-lKFK6jEdhD~pAJd-9ZaIYwmrPMdMYfTPvyviXIOxRox2ycYZwejrA9UuEUiW4LI30A936xs84~7OPlktaAABpPQI8M6ilmESHLHbEIAfmxwc9kFXzugSBQivWab9mL6GKUrHpTqGWs6US1ccfeTAlFSs2h31TbUG6VgbDKdOazE7hytwn1zRBCOfDKZpTLshHAlqWzbUKojqtDIIFwN6lGr-HRvhbf2yyTAHS7I9KgQzUkRQ~HyLaVow967ySeCZ-vpfz-JKJ1xXJRyuhM8lRy1pyNYj8GoMA__&Key-Pair-Id=K27TQMT39R1C8A"

payload = {
    "url": expired_url,
    "source_name": "Test Source"
}

try:
    response = requests.post(url, json=payload)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
    
    data = response.json()
    if data.get('success') is False and "expired" in data.get('message', '').lower():
        print("VERIFICATION PASSED: Correct error message received.")
    else:
        print("VERIFICATION FAILED: Unexpected response.")
except Exception as e:
    print(f"Error: {e}")

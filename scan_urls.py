import json
import urllib.parse
import datetime

file_path = "/Volumes/Transcend/DEAN-new idea/2025-09-22_Blue-Cross-and-Blue-Shield-of-Illinois_index.json"

try:
    with open(file_path, 'r') as f:
        data = json.load(f)

    urls = []
    if 'reporting_structure' in data:
        for structure in data['reporting_structure']:
            if 'in_network_files' in structure:
                for file_info in structure['in_network_files']:
                    if 'location' in file_info:
                        urls.append(file_info['location'])

    print(f"Found {len(urls)} URLs in the file.")
    
    expiration_counts = {}
    valid_urls = []
    current_time = datetime.datetime.now().timestamp()

    for url in urls:
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        
        if 'Expires' in params:
            expires = int(params['Expires'][0])
            expiration_counts[expires] = expiration_counts.get(expires, 0) + 1
            
            if expires > current_time:
                valid_urls.append(url)
        else:
            print(f"No Expires param: {url[:50]}...")

    print("\nExpiration Dates Found:")
    for ts, count in expiration_counts.items():
        dt = datetime.datetime.fromtimestamp(ts)
        status = "VALID" if ts > current_time else "EXPIRED"
        print(f"Timestamp: {ts} ({dt}) - Count: {count} - Status: {status}")

    if valid_urls:
        print(f"\nFound {len(valid_urls)} potentially valid URLs!")
        print(f"Sample valid URL: {valid_urls[0]}")
    else:
        print("\nNo valid URLs found based on expiration timestamp.")

except Exception as e:
    print(f"Error: {e}")

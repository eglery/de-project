import requests
import json

doi = "10.1103/PhysRevLett.115.147001"

# Crossref API endpoint for DOI query
url = f"https://api.crossref.org/works/{doi}"

try:
    # GET request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Extract specific fields
        message = data['message']
        print(f"Type: {message.get('type', 'Not available')}")
        print(f"Publisher: {message.get('publisher', 'Not available')}")
        print(f"Published Online: {message.get('published-online', {}).get('date-parts', 'Not available')}")
        print(f"Is Referenced By Count: {message.get('is-referenced-by-count', 'Not available')}")
        print(f"References Count: {message.get('references-count', 'Not available')}")
        print(f"Subject: {', '.join(message.get('subject', ['Not available']))}")
        
        # Funder information
        funders = message.get('funder', [])
        if funders:
            print("Funders:")
            for funder in funders:
                print(f" - {funder.get('name', 'Unknown')} (DOI: {funder.get('DOI', 'Not available')})")
        else:
            print("Funders: Not available")

    else:
        print(f"Failed to retrieve data: Status code {response.status_code}")

except requests.RequestException as e:
    print(f"Error during request: {e}")

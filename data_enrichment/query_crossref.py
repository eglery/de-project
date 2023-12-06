import requests
import json

# List of DOIs
dois = ["10.1140/epjd/e2018-80782-x", "10.1007/s00023-017-0616-8", "10.1103/PhysRevB.96.045133"]

# Iterate over each DOI in the list
for doi in dois:
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
            print(f"DOI: {doi}")
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
            print(f"Failed to retrieve data for DOI {doi}: Status code {response.status_code}")

    except requests.RequestException as e:
        print(f"Error during request for DOI {doi}: {e}")

    print("\n")  # Adding a new line for better separation between DOIs

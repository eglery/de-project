import requests
import psycopg2

# Database connection parameters - update these with your database details
db_params = {
    'database': 'data_db',
    'user': 'data_user',
    'password': 'data_password',
    'host': 'localhost',
    'port': '8093'
}

# SQL query to fetch DOIs
sql_query = "SELECT doi FROM test_arxiv_table"

try:
    # Connect to the database
    conn = psycopg2.connect(**db_params)

    # Create a cursor object
    cur = conn.cursor()

    # Execute the query
    cur.execute(sql_query)

    # Fetch all rows from the query
    rows = cur.fetchall()

    # Extract DOIs from the rows
    dois = [row[0] for row in rows]

    # Close the cursor and connection
    cur.close()
    conn.close()

except psycopg2.DatabaseError as e:
    print(f"Database error: {e}")
    exit(1)

# List of test DOIs
#dois = ["10.1140/epjd/e2018-80782-x", "10.1007/s00023-017-0616-8", "10.1103/PhysRevB.96.045133"]

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

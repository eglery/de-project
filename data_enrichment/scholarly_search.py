from scholarly import scholarly

query_title = "High Tc superconductivity at the interface between the CaCuO2 and SrTiO3 insulating oxides"

search_query = scholarly.search_pubs(query_title)

# Fetch the first article
try:
    article = next(search_query)
    #num_citations = article.get('num_citations', 'Citation count not available')
    #print(f"Number of citations: {num_citations}")
except StopIteration:
    print("No articles found for the given title.")

for key, value in article.items():
    print(f"{key}: {value}\n")



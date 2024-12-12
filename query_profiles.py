import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Configure
import json

# Weaviate credentials
WEAVIATE_URL = "https://ffcysirvtmo4qyfrcd4xrq.c0.us-east1.gcp.weaviate.cloud"
WEAVIATE_API_KEY = "REns3BJHzpWZLl2O7AHIVKqrOuWtp4x7uYMP"  # Admin API key
MISTRAL_API_KEY = "8FyUKg9JXkjqozjsJ1ZrxqcZknolkn0D"

def connect_to_weaviate():
    """Establish connection to Weaviate."""
    try:
        headers = {
            "X-Mistral-Api-Key": MISTRAL_API_KEY
        }
        
        client = weaviate.connect_to_weaviate_cloud(
            cluster_url=WEAVIATE_URL,
            auth_credentials=Auth.api_key(WEAVIATE_API_KEY),
            headers=headers
        )
        print("Successfully connected to Weaviate!")
        return client
    except Exception as e:
        print(f"Error connecting to Weaviate: {e}")
        return None

def get_collection(client):
    """Get the PersonProfile collection."""
    try:
        collection = client.collections.get("PersonProfile")
        print("Successfully connected to PersonProfile collection!")
        return collection
    except Exception as e:
        print(f"Error getting collection: {e}")
        return None

def search_profiles(collection, search_text):
    """Search for profiles and generate insights using Mistral."""
    try:
        # Get profiles related to building
        results = collection.query.near_text(
            query=search_text,
            limit=3
        ).with_additional([
            "id",
            "distance",
            "vector"
        ]).do()

        if 'data' in results and 'Get' in results['data']:
            profiles = results['data']['Get']['PersonProfile']
            if not profiles:
                print("No profiles found matching the search criteria.")
                return None
            return profiles
        return None
    except Exception as e:
        print(f"Error searching profiles: {e}")
        return None

def generate_insights(client, profiles):
    """Generate insights using Mistral about the profiles."""
    try:
        # Prepare the context from profiles
        context = "\n\n".join([p['content'] for p in profiles])
        
        # Generate individual insights
        print("\nIndividual insights for each profile:")
        for profile in profiles:
            response = client.generate.generate_text(
                collection="PersonProfile",
                prompt=f"Analyze this person's profile and tell me if they are interested in building things. What kind of projects might they be working on?\n\nProfile:\n{profile['content']}",
                generative_config=Configure.Generative.mistral(model="mistral-medium")
            )
            print(f"\nProfile: {profile['content']}")
            print(f"Generated Insight: {response}")

        # Generate group analysis
        print("\nGroup analysis:")
        group_response = client.generate.generate_text(
            collection="PersonProfile",
            prompt=f"Based on these profiles, who seems most interested in building things and what kind of builder are they?\n\nProfiles:\n{context}",
            generative_config=Configure.Generative.mistral(model="mistral-medium")
        )
        print(f"\nGroup Analysis: {group_response}")

    except Exception as e:
        print(f"Error generating insights: {e}")

if __name__ == "__main__":
    # Connect to Weaviate
    client = connect_to_weaviate()
    if client is None:
        exit(1)

    # Get collection
    collection = get_collection(client)
    if collection is None:
        exit(1)

    # Search for profiles interested in building
    print("\nSearching for profiles interested in building:")
    profiles = search_profiles(collection, "building")
    
    if profiles:
        # Generate insights using Mistral
        generate_insights(client, profiles)

    # Close the client
    client.close()

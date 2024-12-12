import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Configure
import json
import os

# Weaviate credentials from environment variables
WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")  # Admin API key
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")

def connect_to_weaviate():
    """Establish connection to Weaviate."""
    try:
        client = weaviate.connect_to_weaviate_cloud(
            cluster_url=WEAVIATE_URL,
            auth_credentials=Auth.api_key(WEAVIATE_API_KEY)
        )
        print("Successfully connected to Weaviate!")
        return client
    except Exception as e:
        print(f"Error connecting to Weaviate: {e}")
        return None

def get_or_create_collection(client):
    """Get existing collection or create a new one."""
    try:
        # Create new collection
        collection = client.collections.create(
            name="PersonProfile",
            vectorizer_config=Configure.Vectorizer.text2vec_weaviate(),
            properties=[
                {
                    "name": "content",
                    "data_type": ["text"],
                    "description": "The full content of the person's profile"
                }
            ]
        )
        print("Successfully created new PersonProfile collection!")
        return collection
    except Exception as e:
        if "already exists" in str(e):
            print("Collection already exists, connecting to it...")
            return client.collections.get("PersonProfile")
        print(f"Error with collection: {e}")
        return None

def add_person(collection, profile_data):
    """Add a person's profile to the collection."""
    try:
        # Convert profile data to a single content string
        content = f"""Profile: {profile_data['username']}
Description: {profile_data['description']}
Profile Link: {profile_data['profile_link']}
Joined: {profile_data['joined_date']}
Events Hosted: {profile_data['hosted']}
Events Attended: {profile_data['attended']}
Social Links: {', '.join(profile_data['social_links'])}"""

        collection.data.insert({
            "content": content
        })
        print(f"Successfully added profile for {profile_data['username']}")
    except Exception as e:
        print(f"Error adding profile: {e}")

if __name__ == "__main__":
    # Connect to Weaviate
    client = connect_to_weaviate()
    if client is None:
        exit(1)

    # Get or create collection
    collection = get_or_create_collection(client)
    if collection is None:
        exit(1)

    # Example profile data
    example_profile = {
        "profile_link": "https://lu.ma/user/itsajchan",
        "description": "Yo, let's build epic stuff together!",
        "username": "Adam Chan @itsajchan",
        "joined_date": "August 2023",
        "hosted": 22,
        "attended": 51,
        "social_links": [
            "https://x.com/itsajchan",
            "https://linkedin.com/in/itsajchan",
            "https://youtube.com/@itsajchan"
        ]
    }

    # Add the example profile
    add_person(collection, example_profile)

    # Close the client
    client.close()

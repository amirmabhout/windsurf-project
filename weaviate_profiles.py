import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Configure, Property
import json
import os

# Weaviate credentials
WEAVIATE_URL = "https://avd3y1rdrjmkopa8iifnlg.c0.us-east1.gcp.weaviate.cloud"
WEAVIATE_API_KEY = "tSJdGE9PO1U9N2kVU85XMtsUX6C33t7kCiSi"
OPENAI_API_KEY = "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"

def connect_to_weaviate():
    """Establish connection to Weaviate."""
    try:
        headers = {
            "X-OpenAI-Api-Key": OPENAI_API_KEY
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

def create_person_schema(client):
    """Create the schema for storing person profiles."""
    try:
        # Delete the collection if it already exists
        if client.collections.exists("PersonProfile"):
            client.collections.delete("PersonProfile")
        
        # Create a new collection with text2vec-weaviate vectorizer and GPT-4 generation
        person_collection = client.collections.create(
            name="PersonProfile",
            vectorizer_config=Configure.Vectorizer.text2vec_weaviate(),
            generative_config=Configure.Generative.openai(
                model="gpt-4"
            ),
            properties=[
                Property(
                    name="content",
                    data_type=Property.DataType.TEXT,
                    description="The full content of the person's profile"
                )
            ]
        )
        print("Successfully created PersonProfile schema!")
        return person_collection
    except Exception as e:
        print(f"Error creating schema: {e}")
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

def search_and_generate(collection, search_text):
    """Search for profiles and generate insights using GPT-4."""
    try:
        result = (
            collection.generate.near_text(
                query=search_text,
                grouped_task="Analyze this person's profile and provide insights about their interests and activities. What kind of collaborations might they be interested in?",
                limit=5
            )
        )
        return result
    except Exception as e:
        print(f"Error searching profiles: {e}")
        return None

if __name__ == "__main__":
    # Connect to Weaviate
    client = connect_to_weaviate()
    if not client:
        exit(1)

    # Create schema
    collection = create_person_schema(client)
    if not collection:
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

    # Search and generate insights
    print("\nSearching for profiles and generating insights about 'building and collaboration':")
    results = search_and_generate(collection, "building and collaboration")
    if results:
        print(f"\nGenerated Insights: {results.generated}")
        for obj in results.objects:
            print(f"\nProfile Content: {obj.properties['content']}")

    # Close the client
    client.close()

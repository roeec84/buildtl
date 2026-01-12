"""
Quick script to check ChromaDB collections and their contents.
"""
import chromadb

# Connect to ChromaDB
client = chromadb.HttpClient(host="localhost", port=8001)

try:
    # List all collections
    collections = client.list_collections()

    print("=" * 60)
    print("CHROMADB COLLECTIONS")
    print("=" * 60)

    if not collections:
        print("No collections found!")
    else:
        for collection in collections:
            print(f"\nCollection: {collection.name}")
            print(f"  Count: {collection.count()} documents")

            # Get a few sample documents
            if collection.count() > 0:
                results = collection.get(limit=3, include=['metadatas', 'documents'])
                print(f"  Sample documents:")
                for i, (doc, metadata) in enumerate(zip(results['documents'], results['metadatas'])):
                    print(f"    [{i+1}] {doc[:100]}...")
                    print(f"        Metadata: {metadata}")

    print("\n" + "=" * 60)

except Exception as e:
    print(f"Error connecting to ChromaDB: {e}")
    print("Make sure ChromaDB is running on localhost:8001")

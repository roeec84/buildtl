"""
Script to migrate documents from default collection to roeecv collection.
"""
import chromadb

# Connect to ChromaDB
client = chromadb.HttpClient(host="localhost", port=8001)

try:
    # Get source and destination collections
    default_collection = client.get_collection("default")
    roeecv_collection = client.get_collection("roeecv")

    print("=" * 60)
    print("MIGRATING DOCUMENTS")
    print("=" * 60)

    # Get all documents from default collection
    results = default_collection.get(include=['documents', 'metadatas', 'embeddings'])

    if not results['documents']:
        print("No documents to migrate!")
    else:
        print(f"Found {len(results['documents'])} documents in 'default' collection")
        print(f"Migrating to 'roeecv' collection...")

        # Add documents to roeecv collection
        roeecv_collection.add(
            ids=results['ids'],
            documents=results['documents'],
            metadatas=results['metadatas'],
            embeddings=results['embeddings']
        )

        print(f"✓ Migrated {len(results['documents'])} documents to 'roeecv'")

        # Delete default collection
        client.delete_collection("default")
        print("✓ Deleted 'default' collection")

    print("=" * 60)
    print("Migration complete!")

except Exception as e:
    print(f"Error during migration: {e}")
    import traceback
    traceback.print_exc()

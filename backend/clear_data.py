"""
Script to clear all ChromaDB collections and PostgreSQL data sources.
"""
import chromadb
import psycopg2
from app.core.config import settings

def clear_chromadb():
    """Clear all ChromaDB collections."""
    try:
        client = chromadb.HttpClient(host="localhost", port=8001)
        collections = client.list_collections()

        print("=" * 60)
        print("CLEARING CHROMADB COLLECTIONS")
        print("=" * 60)

        if not collections:
            print("No collections found!")
        else:
            for collection in collections:
                print(f"Deleting collection: {collection.name}")
                client.delete_collection(collection.name)
            print(f"\nDeleted {len(collections)} collections")

        print("=" * 60)

    except Exception as e:
        print(f"Error clearing ChromaDB: {e}")

def clear_postgres_data_sources():
    """Clear all data sources from PostgreSQL."""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD
        )
        cursor = conn.cursor()

        print("\n" + "=" * 60)
        print("CLEARING POSTGRESQL DATA SOURCES")
        print("=" * 60)

        # Count existing data sources
        cursor.execute("SELECT COUNT(*) FROM data_sources;")
        count = cursor.fetchone()[0]
        print(f"Found {count} data sources")

        # Delete all data sources
        cursor.execute("DELETE FROM data_sources;")
        conn.commit()

        print(f"Deleted {count} data sources")
        print("=" * 60)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error clearing PostgreSQL: {e}")

if __name__ == "__main__":
    clear_chromadb()
    clear_postgres_data_sources()
    print("\n✓ Cleanup complete!")

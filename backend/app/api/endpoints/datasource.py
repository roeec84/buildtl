"""
Data source processing endpoints for GitHub, URL, and text sources.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.db.database import get_db
from app.models.user import User
from app.models.settings import DataSource
from app.api.deps import get_current_active_user
from app.services.vector_store_service import VectorStoreFactory
from app.services.file_service import FileService
from langchain_community.document_loaders import GitLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from app.core.security import decrypt_value
import os


router = APIRouter(prefix="/api/datasources", tags=["Data Sources"])


@router.post("/{data_source_id}/process")
async def process_data_source(
    data_source_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Process a data source (GitHub, URL, text) and add it to the vector store.

    Args:
        data_source_id: ID of the data source to process
        current_user: Authenticated user
        db: Database session

    Returns:
        Success message with processing details
    """
    # Get data source
    result = await db.execute(
        select(DataSource)
        .where(DataSource.id == data_source_id)
        .where(DataSource.user_id == current_user.id)
    )
    data_source = result.scalar_one_or_none()

    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )

    try:
        print(f"DEBUG: Processing data source {data_source.id} of type '{data_source.type}' len={len(data_source.type)}")
        documents = []

        documents = []
        source_type = data_source.type.lower().strip()
        
        if source_type == "github":
            # Process GitHub repository
            repo_url = data_source.config.get("repo_url")
            if not repo_url:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Repository URL is required for GitHub data sources"
                )

            # Extract owner/repo from URL
            # Example: https://github.com/owner/repo -> owner/repo
            repo_url = repo_url.rstrip("/")

            if repo_url.endswith(".git"):
                repo_url = repo_url[:-4]

            parts = repo_url.split("/")
            if len(parts) < 2:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid GitHub URL format. Expected: https://github.com/owner/repo"
                )
            
            
            repo_name = f"{parts[-2]}/{parts[-1]}"
            access_token = data_source.config.get("token")
            # Try to decrypt if it looks like an encrypted string (or just try generic decrypt)
            if access_token:
                try:
                    access_token = decrypt_value(access_token)
                except Exception:
                    # If decryption fails, assume it's plaintext (migration/legacy case)
                    pass

            if not access_token:
                 access_token = os.getenv("GITHUB_TOKEN")

            branch = data_source.config.get("branch", "main")

            print(f"[GIT LOADER] Processing repo: {repo_name} on branch: {branch}")
            
            # Construct clone URL with token if available
            if access_token:
                clone_url = f"https://{access_token}@github.com/{repo_name}.git"
            else:
                clone_url = f"https://github.com/{repo_name}.git"

            # Use LangChain GitLoader with temporary directory
            import tempfile
            from langchain_community.document_loaders import GitLoader
            
            with tempfile.TemporaryDirectory() as temp_dir:
                print(f"[GIT LOADER] Cloning to temporary directory: {temp_dir}")
                loader = GitLoader(
                    clone_url=clone_url,
                    repo_path=temp_dir,
                    branch=branch,
                )
                documents = loader.load()
                print(f"[GIT LOADER] Loaded {len(documents)} documents from {repo_name}")

        elif source_type == "url":
            # Process URL
            url = data_source.config.get("url")
            if not url:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="URL is required for URL data sources"
                )

            # Use simple web scraping
            from langchain_community.document_loaders import WebBaseLoader
            loader = WebBaseLoader(url)
            documents = loader.load()

        elif source_type == "text":
            # Process text content
            text = data_source.config.get("text")
            if not text:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Text content is required for text data sources"
                )

            # Create a document from the text
            from langchain.schema import Document
            documents = [Document(page_content=text, metadata={"source": "text_input"})]

        elif source_type == "sql":
            # Process SQL database
            config = data_source.config
            engine = config.get("engine")
            host = config.get("host")
            port = config.get("port")
            database = config.get("database")
            username = config.get("username")
            password = config.get("password")
            
            # Retrieve optional query or tables
            custom_query = config.get("query")
            credentials_json = config.get("credentials_json")
            
            # Validation logic
            if engine == "bigquery":
                if not all([engine, host, database, credentials_json]):
                     raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Engine, Project ID (Host), Dataset ID (Database), and Service Account JSON are required for BigQuery"
                    )
            else:
                if not all([engine, host, database, username]):
                     raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Engine, Host, Database, and Username are required for SQL data sources"
                    )
            
            # Construct connection string
            # Basic mapping for common engines
            drivers = {
                "postgresql": "postgresql+psycopg2",
                "mysql": "mysql+pymysql", 
                "mssql": "mssql+pyodbc", 
                "sqlite": "sqlite",
                "bigquery": "bigquery",
            }
            
            driver = drivers.get(engine, engine)
            
            if engine == "sqlite":
                 # sqlite:///path/to/db
                 uri = f"sqlite:///{host}"
            elif engine == "bigquery":
                 # bigquery://project/dataset
                 # We map "host" field to "project" and "database" field to "dataset"
                 uri = f"bigquery://{host}/{database}"
                 
                 credentials_info = None
                 
                 if credentials_json:
                     try:
                         import json
                         from google.oauth2 import service_account
                         
                         # Parse the JSON string
                         info = json.loads(credentials_json)
                         
                         # Create credentials object
                         credentials_info = service_account.Credentials.from_service_account_info(info)
                         print(f"[BIGQUERY] Loaded credentials for project: {info.get('project_id')}")
                         
                     except ImportError:
                         print("[BIGQUERY ERROR] google-auth not installed")
                         raise HTTPException(
                             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                             detail="BigQuery dependencies missing (google-auth)"
                         )
                     except json.JSONDecodeError:
                         raise HTTPException(
                             status_code=status.HTTP_400_BAD_REQUEST,
                             detail="Invalid JSON format for Service Account credentials"
                         )
                     except Exception as e:
                         print(f"[BIGQUERY ERROR] Failed to load credentials: {str(e)}")
                         raise HTTPException(
                             status_code=status.HTTP_400_BAD_REQUEST,
                             detail=f"Invalid Service Account credentials: {str(e)}"
                         )
            else:
                # dialect+driver://username:password@host:port/database
                # Handle password escaping if needed, for MVP simple string formatting
                uri = f"{driver}://{username}:{password}@{host}:{port}/{database}"

            from langchain_community.utilities import SQLDatabase
            from langchain_community.document_loaders import SQLDatabaseLoader
            from sqlalchemy import create_engine
            
            try:
                # Special handling for BigQuery with custom credentials
                if engine == "bigquery" and credentials_info:
                    # Create engine with custom credentials
                    # sqlalchemy-bigquery accepts credentials_info in create_engine or args
                    # We pass 'credentials_info' directly to create_engine which passes it to bigquery client
                    engine_instance = create_engine(uri, credentials_info=info)
                    db_instance = SQLDatabase(engine_instance)
                else:
                    db_instance = SQLDatabase.from_uri(uri)
                
                if custom_query:
                    print(f"[SQL LOADER] Loading from custom query: {custom_query}")
                    loader = SQLDatabaseLoader(
                        query=custom_query,
                        db=db_instance
                    )
                else:
                    # If no query, try to load all tables? Or maybe limit to specific tables if provided
                    # For MVP, let's just attempt to load everything or fail if no query/table strategy (maybe default to loading all accessible tables is too dangerous?)
                    # Let's verify what the loader does by default: it usually requires a query or table_names.
                    # If user didn't provide specific tables, we might just grab table names and iterate?
                    # For safety, let's require a query for now if we want specific data, OR we could list tables.
                    # Let's just load all tables content if no query 
                    # Actually SQLDatabaseLoader requires 'query' or 'table_name'. It doesn't auto-crawl everything easily in one go without looping.
                    # Let's update logic: require a query OR load from specific tables if we implement table selection later.
                    # For this MVP, let's support 'query'. If simpler, we can just ask user for a query.
                    # WAIT: The plan said "Query/Tables". Let's assume we support a custom query for flexibility.
                    if not custom_query:
                         # Fallback: try to load all tables (simple approach)
                        tables = db_instance.get_usable_table_names()
                        print(f"[SQL LOADER] No query provided. Found tables: {tables}")
                        # We need to create a loader for EACH table or one big query? 
                        # SQLDatabaseLoader takes ONE query or table.
                        # Let's iterate tables
                        documents = []
                        for table in tables:
                            print(f"[SQL LOADER] Loading table: {table}")
                            try:
                                loader = SQLDatabaseLoader(
                                    query=f"SELECT * FROM {table}",
                                    db=db_instance
                                )
                                documents.extend(loader.load())
                            except Exception as table_err:
                                print(f"[SQL LOADER] Failed to load table {table}: {table_err}")
                        
                        if not documents:
                             raise HTTPException(
                                status_code=status.HTTP_400_BAD_REQUEST,
                                detail="No data found or no valid tables to load."
                            )
                
                if custom_query:
                     documents = loader.load()

            except Exception as sql_err:
                print(f"[SQL ERROR] {str(sql_err)}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to connect to SQL database: {str(sql_err)}"
                )

        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported data source type: {data_source.type}"
            )

        # Split documents into chunks
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            length_function=len,
        )
        chunks = text_splitter.split_documents(documents)
        print(f"[CHUNKING] Created {len(chunks)} chunks from {len(documents)} documents")

        # Get vector store
        vector_store = VectorStoreFactory.create_for_user(
            user_id=current_user.id,
            collection_name=data_source.name  # name already includes username prefix
        )
        print(f"[VECTOR STORE] Using collection: {data_source.name}")

        # Prepare texts and metadatas
        texts = [chunk.page_content for chunk in chunks]
        metadatas = [
            {
                **chunk.metadata,
                "data_source_id": data_source.id,
                "data_source_name": data_source.display_name,
                "user_id": current_user.id,
            }
            for chunk in chunks
        ]
        print(f"[VECTOR STORE] Prepared {len(texts)} texts and {len(metadatas)} metadatas")

        # Add documents to vector store
        doc_ids = await vector_store.add_documents(
            texts=texts,
            metadatas=metadatas
        )
        print(f"[VECTOR STORE] Added {len(doc_ids)} documents to vector store")

        return {
            "message": "Data source processed successfully",
            "data_source_id": data_source.id,
            "data_source_name": data_source.display_name,
            "type": data_source.type,
            "documents_processed": len(documents),
            "chunks_created": len(chunks),
            "collection": data_source.name
        }

    except Exception as e:
        import traceback
        print(f"[DATA SOURCE PROCESSING ERROR] {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing data source: {str(e)}"
        )

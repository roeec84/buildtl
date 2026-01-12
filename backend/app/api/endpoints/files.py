"""
File upload endpoints for document processing and RAG.
"""
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from app.models.user import User
from app.api.deps import get_current_active_user
from app.services.file_service import FileService
from app.services.vector_store_service import VectorStoreFactory
from app.core.config import settings


router = APIRouter(prefix="/api/files", tags=["Files"])


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    collection_name: str = Form(...),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Upload a file and add it to the vector store for RAG.

    Args:
        file: The uploaded file
        collection_name: Name of the vector store collection
        current_user: Authenticated user
        db: Database session

    Returns:
        Success message with file details
    """
    file_service = FileService()

    try:
        # Validate file size
        contents = await file.read()
        if len(contents) > settings.MAX_UPLOAD_SIZE:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File size exceeds maximum allowed size of {settings.MAX_UPLOAD_SIZE} bytes"
            )

        # Reset file pointer
        await file.seek(0)

        # Save file
        file_path = await file_service.save_upload_file(file, current_user.username)

        # Extract text
        text = await file_service.extract_text(file_path)

        # Chunk text
        chunks = await file_service.chunk_text(text)

        # Normalize collection name by removing spaces and add username prefix
        normalized_collection_name = collection_name.replace(" ", "")
        # Add username prefix to make collection unique per user
        collection_with_user = f"{current_user.username}_{normalized_collection_name}"

        # Add to vector store
        vector_store = VectorStoreFactory.create_for_user(
            user_id=current_user.id,
            collection_name=collection_with_user
        )

        # Create metadata for each chunk
        metadatas = [
            {
                "filename": file.filename,
                "user_id": current_user.id,
                "chunk_index": i
            }
            for i in range(len(chunks))
        ]

        # Add documents to vector store
        doc_ids = await vector_store.add_documents(
            texts=chunks,
            metadatas=metadatas
        )

        return {
            "message": "File uploaded and processed successfully",
            "filename": file.filename,
            "file_path": file_path,
            "chunks_created": len(chunks),
            "collection": collection_with_user
        }

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=str(e)
        )
    except Exception as e:
        import traceback
        print(f"[FILE UPLOAD ERROR] {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing file: {str(e)}"
        )

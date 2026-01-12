from langchain_huggingface.embeddings import HuggingFaceEmbeddings

# Define the model name and optional parameters
model_name = "sentence-transformers/all-mpnet-base-v2" # A popular, performant model
model_kwargs = {'device': 'cpu'} # Specify 'cuda' if you have a compatible GPU
encode_kwargs = {'normalize_embeddings': False} # Optional: set to True if normalization is needed

# Initialize the HuggingFaceEmbeddings instance
hf_embeddings = HuggingFaceEmbeddings(
    model_name=model_name,
    model_kwargs=model_kwargs,
    encode_kwargs=encode_kwargs
)

# Example text to embed
text_documents = [
    "This is a test document.",
    "Embeddings are the core of modern LLM-powered applications."
]

# Generate embeddings for documents or a single query
doc_embeddings = hf_embeddings.embed_documents(text_documents)
query_embedding = hf_embeddings.embed_query("What is the meaning of life?")

print(f"Embedding for document 1 (length {len(doc_embeddings[0])}): {doc_embeddings[0][:5]}...")
print(f"Embedding for query (length {len(query_embedding)}): {query_embedding[:5]}...")

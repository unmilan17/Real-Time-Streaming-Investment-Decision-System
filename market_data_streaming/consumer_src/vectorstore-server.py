import os
from .pipeline import run_pipeline
from pathway.xpacks.llm import embedders
from pathway.xpacks.llm.vector_store import VectorStoreServer

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "your-gemini-api-key")
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", 8756))
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER", "./output")

def start_vectorstore_server():
    documents = run_pipeline()

    embedder = embedders.GeminiEmbedder(api_key=GEMINI_API_KEY)
    vector_server = VectorStoreServer(documents, embedder=embedder)

    print(f"🚀 MCP Server running at {MCP_HOST}:{MCP_PORT}")
    vector_server.run_server(host=MCP_HOST, port=MCP_PORT)

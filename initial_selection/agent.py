import pathway as pw
from pathway.stdlib.indexing.nearest_neighbors import BruteForceKnnFactory
from pathway.xpacks.llm import llms
from pathway.xpacks.llm.document_store import DocumentStore
from pathway.xpacks.llm.embedders import OpenAIEmbedder
from pathway.xpacks.llm.parsers import UnstructuredParser
from pathway.xpacks.llm.splitters import TokenCountSplitter


class BaseAgent:

    def __init__(self, name: str, llm: llms.LLM, embedder: OpenAIEmbedder):
        self.name = name
        self.llm = llm
        self.embedder = embedder
        self.parser = UnstructuredParser()
        self.splitter = TokenCountSplitter(max_tokens=500)
        self.vector_store = DocumentStore(
            embedder=self.embedder,
            knn_factory=BruteForceKnnFactory(),
        )


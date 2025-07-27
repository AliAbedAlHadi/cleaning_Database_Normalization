import os
from typing import List, Dict, Optional, Union
from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.docstore.document import Document
from dotenv import load_dotenv

load_dotenv()

BASE_CHROMA_DIR = "app/vectordb"

def get_embedding_model():
    return HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

def get_chroma_dir(db_name: str) -> str:
    # Per-database vector store directory
    return os.path.join(BASE_CHROMA_DIR, f"chroma_db_{db_name}")

def format_schema(schema: List[Dict]) -> List[Document]:
    tables = {}
    for row in schema:
        table = row["table_name"]
        col = f"{row['column_name']} ({row['data_type']})"
        if row.get("is_primary_key"):
            col += " [PK]"
        if row.get("is_foreign_key"):
            col += f" [FK → {row['referenced_table']}.{row['referenced_column']}]"
        tables.setdefault(table, []).append(col)

    docs = []
    for table, cols in tables.items():
        content = f"Table: {table}\nColumns:\n" + "\n".join(f" - {col}" for col in cols)
        docs.append(Document(page_content=content))
    return docs

def embed_schema(schema: List[Dict], db_name: str):
    documents = format_schema(schema)
    
    # Add special marker doc to indicate schema embedded for this DB
    marker_doc = Document(page_content=f"SCHEMA_EMBEDDED:{db_name}")
    documents.append(marker_doc)
    
    splitter = RecursiveCharacterTextSplitter(chunk_size=512, chunk_overlap=50)
    split_docs = splitter.split_documents(documents)

    embeddings = get_embedding_model()
    vectordb = Chroma.from_documents(
        split_docs,
        embedding=embeddings,
        persist_directory=get_chroma_dir(db_name)
    )
    # No need to call vectordb.persist() in recent versions
    return True

def is_schema_already_embedded(db_name: str, k: int = 1) -> bool:
    embeddings = get_embedding_model()
    vectordb = Chroma(
        persist_directory=get_chroma_dir(db_name),
        embedding_function=embeddings
    )

    retriever = vectordb.as_retriever(search_kwargs={"k": k})
    results = retriever.invoke(f"SCHEMA_EMBEDDED:{db_name}")
    return len(results) > 0

def retrieve_schema_chunks(
    db_name: str,
    query: Optional[Union[str, List[str]]] = None,
    k: int = 4
) -> List[str]:
    print(f"[DEBUG] Starting retrieve_schema_chunks with db_name='{db_name}', query={query}, k={k}")
    embeddings = get_embedding_model()
    print("[DEBUG] Got embeddings model")

    vectordb_dir = get_chroma_dir(db_name)
    print(f"[DEBUG] Using Chroma directory: {vectordb_dir}")

    vectordb = Chroma(
        persist_directory=vectordb_dir,
        embedding_function=embeddings
    )
    print("[DEBUG] Initialized Chroma vectorstore")

    if query is None:
        print("[DEBUG] Query is None, retrieving all documents")
        all_docs = vectordb._collection.get(include=["documents"])
        print(f"[DEBUG] Retrieved {len(all_docs['documents'])} documents")
        return [doc.page_content for doc in all_docs["documents"]]

    if isinstance(query, list):
        print(f"[DEBUG] Query is a list with {len(query)} items")
        retriever = vectordb.as_retriever(search_kwargs={"k": k})
        chunks = []
        for q in query:
            print(f"[DEBUG] Invoking retriever for query: 'Table: {q}'")
            results = retriever.invoke(f"Table: {q}")
            print(f"[DEBUG] Retrieved {len(results)} chunks for '{q}'")
            chunks.extend(doc.page_content for doc in results)
        print(f"[DEBUG] Total chunks collected: {len(chunks)}")
        return chunks

    print(f"[DEBUG] Query is a string: '{query}'")
    retriever = vectordb.as_retriever(search_kwargs={"k": k})
    results = retriever.invoke(query)
    print(f"[DEBUG] Retrieved {len(results)} chunks for query '{query}'")
    return [doc.page_content for doc in results]

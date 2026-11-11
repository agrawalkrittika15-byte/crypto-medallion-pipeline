import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from qdrant_client import QdrantClient
from qdrant_client.http import models
import clickhouse_connect
import ollama
from apscheduler.schedulers.background import BackgroundScheduler

# Initialize Qdrant Client
qdrant_client = QdrantClient(host="localhost", port=6333)
COLLECTION_NAME = "crypto_analytics_docs"

# Initialize Scheduler for scheduled ingestion
scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Ensure Qdrant collection exists and start twice-daily ingestion job."""
    collections = [c.name for c in qdrant_client.get_collections().collections]
    if COLLECTION_NAME not in collections:
        qdrant_client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=models.VectorParams(size=768, distance=models.Distance.COSINE),
        )
    
    # Schedule sync_clickhouse_to_qdrant to run twice a day (every 12 hours)
    scheduler.add_job(sync_clickhouse_to_qdrant, 'interval', hours=12)
    scheduler.start()
    
    yield
    
    scheduler.shutdown()

app = FastAPI(title="Crypto Medallion RAG API", lifespan=lifespan)

# Define User Request Schema
class QueryRequest(BaseModel):
    question: str

# Refactored Guardrail System Prompt
SYSTEM_PROMPT = """You are an AI Analytics Assistant for the Crypto Medallion Warehouse.
Answer user questions concisely and directly using only the provided crypto database context.

Strict Rule: If the query is completely unrelated to cryptocurrency, market data, or database metrics (e.g., food, sports, general knowledge), REJECT it by replying:
I am strictly configured to answer questions about the Crypto Medallion Warehouse Analytics. Please ask a relevant data warehouse or cryptocurrency question.

Context from database:
{context}

User Question: {question}
Answer:"""

def generate_embedding(text: str):
    """Generate vector embedding using nomic-embed-text."""
    res = ollama.embeddings(model="nomic-embed-text", prompt=text)
    return res["embedding"]

@app.post("/ingest")
def sync_clickhouse_to_qdrant():
    """Fetch analytics context from ClickHouse and ingest embeddings into Qdrant."""
    try:
        ch_client = clickhouse_connect.get_client(
            host="localhost",
            port=8123,
            username="default",
            password="cryptopass123"
        )
        
        result = ch_client.query("SELECT * FROM crypto_pipeline.gold_crypto_summary LIMIT 50")
        rows = result.result_rows
        col_names = result.column_names
        
        points = []
        for idx, row in enumerate(rows):
            row_dict = dict(zip(col_names, row))
            doc_text = "Crypto Summary Record: " + ", ".join([f"{k}: {v}" for k, v in row_dict.items()])
            
            embedding = generate_embedding(doc_text)
            
            points.append(models.PointStruct(
                id=idx + 1,
                vector=embedding[:768],
                payload={"text": doc_text, "symbol": str(row_dict.get("symbol", ""))}
            ))

        if points:
            qdrant_client.upsert(collection_name=COLLECTION_NAME, points=points)

        return {"status": "success", "ingested_records": len(points)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ask")
def query_rag_pipeline(req: QueryRequest):
    """Main RAG Endpoint with context retrieval and strict domain guardrails."""
    try:
        question = req.question

        # 1. Generate query embedding
        q_emb = generate_embedding(question)[:768]

        # 2. Retrieve top-k context vectors
        search_results = qdrant_client.query_points(
            collection_name=COLLECTION_NAME,
            query=q_emb,
            limit=3
        ).points

        retrieved_context = "\n".join([hit.payload["text"] for hit in search_results if hit.payload and "text" in hit.payload])

        # 3. Pass context and question to Ollama
        full_prompt = SYSTEM_PROMPT.format(context=retrieved_context, question=question)

        response = ollama.generate(
            model="llama3.2:1b",
            prompt=full_prompt
        )

        return {
            "question": question,
            "answer": response["response"].strip(),
            "retrieved_context": retrieved_context
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
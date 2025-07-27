from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from databasefunctions import list_databases, get_and_embed_full_schema
from analyzecleaning import (
    get_tables_list, get_next_table,
    analyze_table, apply_fix, skip_table
)
from normalize import analyze_normalization, apply_normalization
from schema import suggest_schema_improvements, apply_schema_fix


app = FastAPI()

# Enable CORS for all origins, methods, and headers
# Enable CORS for all origins, methods, and headers
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ----- MODELS -----
    # ...existing code...

# ----- MODELS -----
class TableRequest(BaseModel):
    database: str
    table_name: str
    user_question: Optional[str] = None

class FixRequest(BaseModel):
    database: str
    table_name: str
    fix_description: str

class NormalizeApplyRequest(BaseModel):
    database: str
    sql_statements: str

class DatabaseRequest(BaseModel):
    database: str

class SchemaFixRequest(BaseModel):
    database: str
    sql_statements: str

# ----- ROUTES -----
@app.get("/")
def hello():
    return {"message": "Hello from RAG AI Data Analysis backend!"}

@app.get("/databases")
def get_databases():
    return list_databases()

@app.get("/tables/{database}")
def list_tables(database: str):
    return get_tables_list(database)

@app.get("/next-table/{database}")
def next_table(database: str, current_table: Optional[str] = None):
    return get_next_table(database, current_table)

@app.post("/analyze-table")
def analyze(request: TableRequest):
    return analyze_table(request.database, request.table_name, request.user_question)

@app.post("/apply-fix")
def apply(request: FixRequest):
    return apply_fix(request.database, request.table_name, request.fix_description)

@app.post("/skip-table")
def skip(request: TableRequest):
    return skip_table(request.table_name)

@app.post("/normalize/analyze")
def normalize_schema(request: DatabaseRequest):
    return analyze_normalization(request.database)

@app.post("/normalize/apply")
def apply_normalized_schema(request: NormalizeApplyRequest):
    return apply_normalization(request.database, request.sql_statements)
@app.post("/embed-schema/{database}")
def embed_schema_endpoint(database: str):
    schema_info = get_and_embed_full_schema(database)
    return {"success": True, "message": "Schema embedded for database.", "schema_rows": len(schema_info)}
@app.post("/schema/suggest")
def suggest_schema_route(request: DatabaseRequest):
    """
    Returns AI-generated suggestions for schema improvement,
    including summary, SQL fix, and Mermaid ER diagram.
    """
    return suggest_schema_improvements(request.database)


@app.post("/schema/apply-fix")
def apply_schema_fix_route(request: SchemaFixRequest):
    """
    Applies the AI-suggested schema SQL fix to the database.
    """
    return apply_schema_fix(request.database, request.sql_statements)

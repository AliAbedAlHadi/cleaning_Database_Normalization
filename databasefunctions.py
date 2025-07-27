import pyodbc
import os
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DB_SERVER = os.getenv("DB_SERVER")

def build_connection_string(database: str = "") -> str:
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_SERVER};"
        f"Trusted_Connection=yes;"
    )
    if database:
        conn_str += f"DATABASE={database};"
    return conn_str

def connect_to_db(database: str = ""):
    try:
        conn = pyodbc.connect(build_connection_string(database), timeout=5)
        return conn
    except pyodbc.Error as e:
        raise Exception(f"Database connection failed: {e}")

def execute_query(query: str, database: Optional[str] = None) -> Optional[List[Dict]]:
    db_part = f"Database={database};" if database else ""
    conn_str = f"Driver={{SQL Server}};Server=localhost;{db_part}Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        if not query.strip().lower().startswith("select"):
            conn.commit()
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        else:
            return []
    finally:
        cursor.close()
        conn.close()

def list_databases() -> List[str]:
    try:
        query = """
        SELECT name FROM sys.databases
        WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
        """
        results = execute_query(query, database="master")
        return [row["name"] for row in results]
    except Exception as e:
        raise Exception(f"Failed to list databases: {e}")

def get_schema(db_name: str) -> List[Dict[str, Any]]:
    try:
        query = """
        SELECT 
            t.name AS table_name,
            c.name AS column_name,
            ty.name AS data_type,
            c.max_length,
            c.is_nullable,
            c.column_id,
            ISNULL(pk.is_primary_key, 0) AS is_primary_key,
            ISNULL(fk.is_foreign_key, 0) AS is_foreign_key,
            fk.referenced_table,
            fk.referenced_column
        FROM sys.tables t
        INNER JOIN sys.columns c ON t.object_id = c.object_id
        INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        LEFT JOIN (
            SELECT 
                ic.object_id, ic.column_id, 1 AS is_primary_key
            FROM sys.index_columns ic
            INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            WHERE i.is_primary_key = 1
        ) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
        LEFT JOIN (
            SELECT 
                fkc.parent_object_id AS object_id,
                fkc.parent_column_id AS column_id,
                1 AS is_foreign_key,
                rt.name AS referenced_table,
                rc.name AS referenced_column
            FROM sys.foreign_key_columns fkc
            INNER JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
            INNER JOIN sys.tables rt ON rc.object_id = rt.object_id
        ) fk ON c.object_id = fk.object_id AND c.column_id = fk.column_id
        ORDER BY t.name, c.column_id;
        """

        conn = connect_to_db(db_name)
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        schema_info = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return schema_info
    except pyodbc.Error as e:
        raise Exception(f"Schema retrieval failed: {e}")

# Stub: always return False since no RAG is used anymore
def is_schema_already_embedded(db_name: str, k: int = 1) -> bool:
    return False

# Keep name, remove embedding logic
def get_and_embed_full_schema(db_name: str) -> List[Dict[str, Any]]:
    schema_info = get_schema(db_name)

    # Optional: print schema overview
    current_table = None
    schema_text = "Database schema overview:\n"
    for row in schema_info:
        if row['table_name'] != current_table:
            current_table = row['table_name']
            schema_text += f"\nTable: {current_table}\n"
        schema_text += (
            f"  Column: {row['column_name']} | Type: {row['data_type']} | "
            f"PK: {row['is_primary_key']} | FK: {row['is_foreign_key']} "
            f"(Ref: {row.get('referenced_table', '')}.{row.get('referenced_column', '')})\n"
        )
    print(schema_text)

    print(f"✅ Schema for database '{db_name}' retrieved successfully (no embedding needed).")
    return schema_info

from llm import call_llm
from rag_engine import retrieve_schema_chunks
from databasefunctions import execute_query
from typing import Dict, List

def analyze_normalization(database_name: str) -> Dict:
    """
    Retrieve schema from vector DB and ask LLM to generate normalized SQL.
    Returns SQL only. Does not execute it.
    """

    all_chunks = retrieve_schema_chunks(f"Schema of database {database_name}")
    print(f"Retrieved {len(all_chunks)} schema chunks for normalization {all_chunks}.")
    if not all_chunks:
        return {
            "success": False,
            "message": f"No schema found in vector DB for database '{database_name}'."
        }

    schema_text = "\n\n".join(all_chunks)

    prompt = f"""
You are a senior SQL Server database expert.

Given this current schema below, generate T-SQL statements to normalize the structure.
Fix any issues with:
- Redundant columns
- Missing foreign keys
- Denormalized design
- Unclear relationships or improper types
- working on whole schema not only on one table.
Return only the new T-SQL statements (e.g., CREATE TABLE, ALTER TABLE, ADD CONSTRAINT, DROP/RENAME if needed).

⚠️ IMPORTANT RULES:
- DO NOT return code as a string or wrapped in quotes.
- DO NOT include any markdown code fences (```sql or ```), only pure SQL statements.
- DO NOT drop or delete the entire table unless explicitly instructed.
- ONLY update, alter, or modify the existing table.
- Use `USE [{database_name}];` followed by `GO` before your SQL statements.
- RETURN ONLY executable T-SQL code (no explanations, no comments).
- Be careful with column you make it primary to be not null if null do it not null then do it as primary key same to freign key.
Ensure the SQL is correct and avoids destructive operations unless required.
Use this database: USE [{database_name}]; GO

Schema:
{schema_text}

Only return the SQL. No comments. No explanation. No formatting outside SQL.
"""

    try:
        sql = call_llm(prompt)
        if not sql.strip():
            return {"success": False, "message": "LLM returned empty SQL."}

        return {
            "success": True,
            "database": database_name,
            "sql_to_review": sql.strip()
        }

    except Exception as e:
        return {
            "success": False,
            "message": f"LLM analysis failed: {str(e)}"
        }


def apply_normalization(database_name: str, sql_statements: str) -> Dict:
    """
    Applies provided SQL statements to the database.
    """

    try:
        statements = [stmt.strip() for stmt in sql_statements.split("GO") if stmt.strip()]
        for stmt in statements:
            execute_query(stmt, database_name)

        return {
            "success": True,
            "message": "Schema normalization SQL executed successfully.",
            "executed_count": len(statements)
        }

    except Exception as e:
        return {
            "success": False,
            "message": f"Execution failed: {str(e)}"
        }

from typing import Optional, Dict, List
from databasefunctions import get_schema, execute_query
from llm import call_llm
from rag_engine import embed_schema, retrieve_schema_chunks

processed_tables = set()
skipped_tables = set()

def get_tables_list(database: str) -> List[str]:
    schema = get_schema(database)
    return sorted(set(row['table_name'] for row in schema))


def get_next_table(database: str, current_table: Optional[str] = None) -> Dict:
    tables = get_tables_list(database)
    if current_table is None:
        next_index = 0
    else:
        try:
            next_index = tables.index(current_table) + 1
        except ValueError:
            next_index = 0

    while next_index < len(tables) and (tables[next_index] in processed_tables or tables[next_index] in skipped_tables):
        next_index += 1

    if next_index >= len(tables):
        return {"done": True, "message": "All tables processed or skipped."}

    next_table = tables[next_index]
    schema = [row for row in get_schema(database) if row['table_name'] == next_table]
    sample_rows = execute_query(f"SELECT TOP 10 * FROM [{next_table}]", database)

    return {
        "done": False,
        "table_name": next_table,
        "schema": schema,
        "sample_rows": sample_rows
    }


def analyze_table(database: str, table_name: str, user_question: Optional[str] = None) -> Dict:
    """
    Analyze the table by retrieving schema context from vector DB (Chroma),
    and fallback to DB schema if not found (embed it then).
    Formats sample data in CSV-style to avoid confusion with schema structure.
    """
    # Step 1: Try to retrieve schema context from Chroma
    context_chunks = retrieve_schema_chunks(f"Schema info for table {table_name}")

    # Step 2: If no context found, fallback to DB schema and embed it
    if not context_chunks:
        schema = [row for row in get_schema(database) if row['table_name'] == table_name]
        embed_schema(schema)
        context_chunks = retrieve_schema_chunks(f"Schema info for table {table_name}")

    # Step 3: Get sample rows from the database
    sample_rows = execute_query(f"SELECT TOP 10 * FROM [{table_name}]", database)

    # Step 4: Format sample data as CSV-style text (not JSON)
    if isinstance(sample_rows, list) and sample_rows:
        columns = sample_rows[0].keys()
        header = ", ".join(columns)
        rows_text = "\n".join(
            ", ".join(str(row.get(col, "")) for col in columns)
            for row in sample_rows
        )
        formatted_data = f"{header}\n{rows_text}"
    else:
        formatted_data = "No sample data available."

    # Step 5: Construct the prompt
    base_prompt = f"""
You are a senior database expert specializing in SQL Server. 
Your task is to analyze the schema and sample data of the following table only, without suggesting changes involving other tables.

Focus exclusively on this table's structure and data. 
Identify any normalization issues, data inconsistencies, redundant or duplicate columns/data, 
and propose concise, practical improvements to make the table more efficient and accurate.

Table: {table_name}

Schema:
{chr(10).join(context_chunks)}

Sample data (CSV format):
{formatted_data}

Your explanation should be brief and to the point, avoiding any unrelated or broad database suggestions.
""".strip()


    # Step 6: Add user question if present
    prompt = f"{base_prompt}\n\nUser question: {user_question}" if user_question else base_prompt

    # Step 7: Call the LLM with the prompt
    analysis = call_llm(prompt)

    return {"analysis": analysis}

def apply_fix(database: str, table_name: str, fix_description: str) -> Dict:
    """
    Apply fix using LLM-generated T-SQL and update vector DB with new schema.
    """
    try:
        prompt = f"""
You are a senior T-SQL expert. Your task is to generate valid, executable T-SQL code to apply the following fix to the table "{table_name}" in the database "{database}":

{fix_description}

⚠️ IMPORTANT RULES:
- DO NOT return code as a string or wrapped in quotes.
- DO NOT say `sql =` or include any Python-style syntax.
- DO NOT drop or delete the entire table unless explicitly instructed.
- ONLY update, alter, or modify the existing table.
- Use `USE [{database}];` followed by `GO` before your SQL statements.
- RETURN ONLY executable T-SQL code (no explanations, no comments).
- Be careful with column you make it primary to be not null if null do it not null then do it as primary key same to freign key.
Ensure the SQL is correct and avoids destructive operations unless required.
"""
        print("==== Prompt Sent to LLM ====")
        print(prompt)

        sql_fix = call_llm(prompt)
        print("==== SQL Returned by LLM ====")
        print(sql_fix)

        if not sql_fix.strip():
            return {"success": False, "message": "LLM did not return any SQL fix."}

        statements = [stmt.strip() for stmt in sql_fix.split("GO") if stmt.strip()]
        print("==== Parsed Statements ====")
        for i, stmt in enumerate(statements):
            print(f"Statement {i+1}:\n{stmt}\n---")

        for stmt in statements:
            print(f"Executing:\n{stmt}")
            execute_query(stmt, database)

        processed_tables.add(table_name)

        print("Re-ingesting updated schema into vector DB...")
        new_schema = [row for row in get_schema(database) if row['table_name'] == table_name]
        print("New schema:", new_schema)
        embed_schema(new_schema)

        return {"success": True, "message": "Fix applied successfully and schema updated in vector DB."}

    except Exception as e:
        import traceback
        print("==== ERROR ====")
        traceback.print_exc()
        return {"success": False, "message": f"Failed to apply fix: {e}"}

def skip_table(table_name: str) -> Dict:
    skipped_tables.add(table_name)
    return {"success": True, "message": f"Table '{table_name}' skipped."}

from typing import Dict, List, Set, Optional, Tuple
import re
import json
import time
import requests
from llm import call_llm
from databasefunctions import execute_query, get_schema

# --------------------------- Utility Functions ---------------------------

def normalize_erd_node(node: dict) -> dict:
    node["type"] = "table"
    columns = node.get("columns") or node.get("data") or []
    normalized_columns = []
    for col in columns:
        normalized_col = {
            "name": col.get("name"),
            "type": col.get("type"),
            "isPrimaryKey": bool(col.get("isPrimaryKey") or col.get("primaryKey")),
            "isForeignKey": bool(col.get("isForeignKey") or col.get("foreignKey")),
            "nullable": col.get("nullable", False)
        }
        normalized_columns.append(normalized_col)
    node["columns"] = normalized_columns
    if "data" in node:
        del node["data"]
    return node

def clean_json_string(raw_str: str) -> str:
    cleaned = raw_str.strip()
    # Remove markdown fences
    cleaned = re.sub(r"^```json\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)

    # Find first '{' and match balanced braces to extract one JSON object
    first_brace = cleaned.find('{')
    if first_brace == -1:
        raise ValueError("No JSON object found in the string")

    # Attempt to find the matching closing brace
    brace_count = 0
    for i in range(first_brace, len(cleaned)):
        if cleaned[i] == '{':
            brace_count += 1
        elif cleaned[i] == '}':
            brace_count -= 1
            if brace_count == 0:
                # Extract substring from first '{' to matching '}'
                return cleaned[first_brace:i+1]

    raise ValueError("No complete JSON object found in the string")
def call_llm_single(prompt: str, max_retries=5, base_delay=2) -> str:
    retries = 0
    while retries < max_retries:
        try:
            result = call_llm(prompt)
            if result is None:
                return ""
            return str(result).strip()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait_time = base_delay * (2 ** retries)
                print(f"[WARN] Rate limit hit, retrying after {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1
                continue
            else:
                print(f"[WARN] LLM call failed: {e}")
                return ""
        except Exception as e:
            print(f"[WARN] LLM call failed: {e}")
            return ""
    print("[ERROR] Max retries reached for LLM call.")
    return ""

# ------------------------ Schema Parsing & Fixing ------------------------

def fix_sql_schema(sql_text: str) -> str:
    fixed = re.sub(r'NVARCHAR\(\s*\)', 'NVARCHAR(255)', sql_text, flags=re.IGNORECASE)
    fixed = re.sub(r'VARCHAR\(\s*\)', 'VARCHAR(255)', fixed, flags=re.IGNORECASE)
    fixed = re.sub(r',\s*\)', ')', fixed)
    fixed = re.sub(r',\s*;', ';', fixed)
    return fixed

def split_schema_to_tables(schema_text: str) -> List[str]:
    # More robust pattern to capture each CREATE TABLE block including brackets and semicolon
    pattern = re.compile(r"(CREATE TABLE\s+\[?\w+\]?.*?\);)", re.IGNORECASE | re.DOTALL)
    tables = pattern.findall(schema_text)
    return tables

# ---------------------- Dependency Graph & Chunking ----------------------

def extract_foreign_key_targets(table_sql: str) -> Set[str]:
    fk_pattern = re.compile(r"FOREIGN\s+KEY\s*\([^\)]+\)\s+REFERENCES\s+\[?(\w+)\]?", re.IGNORECASE)
    return set(fk_pattern.findall(table_sql))

def extract_table_name(table_sql: str) -> str:
    match = re.match(r"CREATE TABLE\s+\[?(\w+)\]?", table_sql, re.IGNORECASE)
    return match.group(1) if match else "Unknown"

def build_dependency_graph(tables: List[str]) -> Tuple[Dict[str, Set[str]], Dict[str, str]]:
    graph: Dict[str, Set[str]] = {}
    table_map: Dict[str, str] = {}
    for ddl in tables:
        name = extract_table_name(ddl)
        if name:
            table_map[name] = ddl
            graph[name] = extract_foreign_key_targets(ddl)
    return graph, table_map

def resolve_chunk_tables(graph: Dict[str, Set[str]], table: str, visited: Optional[Set[str]] = None) -> Set[str]:
    if visited is None:
        visited = set()
    if table in visited:
        return visited
    visited.add(table)
    for fk_table in graph.get(table, []):
        resolve_chunk_tables(graph, fk_table, visited)
    return visited

def chunk_tables(graph: Dict[str, Set[str]], table_map: Dict[str, str], max_columns_per_chunk: int = 80) -> List[str]:
    """
    Return one chunk per table (no grouping).
    """
    chunks = []
    for table_name in table_map:
        chunks.append(table_map[table_name])
    return chunks

def chunk_sql_fixes(sql_fixes: str, max_statements_per_chunk: int = 10) -> List[str]:
    statements = [stmt.strip() for stmt in re.split(r"\bGO\b", sql_fixes, flags=re.IGNORECASE) if stmt.strip()]
    chunks = []
    for i in range(0, len(statements), max_statements_per_chunk):
        chunk = "\nGO\n".join(statements[i:i + max_statements_per_chunk])
        chunks.append(chunk)
    return chunks

# ------------------------- JSON & ERD Helpers -------------------------

def fix_json_with_llm(bad_json_str: str) -> str:
    fix_prompt = f"""
The following JSON is invalid or malformed:

{bad_json_str}

Please return a corrected, valid JSON object only, fixing any syntax errors or missing commas.
"""
    return call_llm_single(fix_prompt)

import json
import re

def parse_erd_json(response: str):
    """
    Clean and parse a malformed ERD JSON string into a valid Python object
    with structure: {"nodes": [...], "edges": [...]}
    """
    try:
        # Step 1: Extract the first valid JSON object from the response
        match = re.search(r'\{.*\}', response, re.DOTALL)
        if not match:
            raise ValueError("No valid JSON object found in response.")
        
        json_str = match.group(0)

        # Step 2: Clean common formatting issues
        json_str = json_str.replace("\\'", "'").replace('\\"', '"').strip()

        # Step 3: Convert string to dict
        parsed = json.loads(json_str)

        # Step 4: Fix edge nesting error (if edges are wrongly placed in nodes)
        if isinstance(parsed.get("nodes"), list):
            corrected_nodes = []
            extracted_edges = []
            for node in parsed["nodes"]:
                if "edges" in node:
                    extracted_edges.extend(node["edges"])
                    node.pop("edges")
                corrected_nodes.append(node)
            parsed["nodes"] = corrected_nodes
            # If top-level "edges" is missing, inject it
            if "edges" not in parsed:
                parsed["edges"] = extracted_edges
            else:
                parsed["edges"].extend(extracted_edges)

        # Step 5: Validation
        if "nodes" not in parsed or "edges" not in parsed:
            raise ValueError("Parsed JSON missing required keys: 'nodes' or 'edges'.")

        # Final cleanup: enforce correct structure on columns
        for node in parsed["nodes"]:
            node.setdefault("type", "table")
            for col in node.get("columns", []):
                col.setdefault("nullable", False)
                col.setdefault("isPrimaryKey", False)
                col.setdefault("isForeignKey", False)

        return parsed

    except Exception as e:
        print(f"[ERROR] parse_erd_json: {e}")
        return None

# ------------------------- Main Suggestion Function -------------------------

def suggest_schema_improvements(database_name: str) -> Dict:
    try:
        print(f"[DEBUG] Retrieving schema for: {database_name}")
        schema_data = get_schema(database_name)
        if not schema_data:
            return {"success": False, "message": "Schema data is empty"}

        # Build DDL string from schema data
        schema_text = ""
        current_table = None
        for idx, row in enumerate(schema_data):
            table_name = row.get("table_name")
            if table_name is None:
                print(f"[WARN] Row {idx} missing 'table_name': {row}")
                continue

            # Close previous table definition if new table found
            if table_name != current_table:
                if current_table is not None:
                    schema_text = schema_text.rstrip(",\n") + "\n);\n\n"
                current_table = table_name
                schema_text += f"CREATE TABLE [{current_table}] (\n"

            column_name = row.get("column_name", "UnknownColumn")
            data_type = row.get("data_type", "VARCHAR")
            max_length = abs(row.get("max_length", 0))

            schema_text += f"  [{column_name}] {data_type}"
            if data_type.upper() in ['VARCHAR', 'NVARCHAR']:
                schema_text += f"({max_length if max_length > 0 else 255})"

            is_nullable = row.get("is_nullable", True)
            schema_text += " NOT NULL" if not is_nullable else " NULL"

            if row.get("is_primary_key", False):
                schema_text += " PRIMARY KEY"

            if row.get("is_foreign_key", False):
                ref_table = row.get("ReferenceTable", "") or row.get("referenced_table", "")
                ref_col = row.get("ReferenceColumn", "") or row.get("referenced_column", "")
                if ref_table and ref_col:
                    schema_text += f" FOREIGN KEY REFERENCES {ref_table}({ref_col})"

            schema_text += ",\n"

        # Close last table if any rows exist
        if current_table is not None:
            schema_text = schema_text.rstrip(",\n") + "\n);"

        schema_text = fix_sql_schema(schema_text)

        # Debug output
        print(f"[DEBUG] Constructed schema text:\n{schema_text}")

        # Get summary from LLM
        summary_prompt = (
            f"You are a SQL Server architect. Given the schema:\n\n{schema_text}\n\n"
            "Summarize any normalization issues, redundant columns, or bad foreign key design. Only give a concise summary."
        )
        summary = call_llm_single(summary_prompt) or ""
        print(f"[DEBUG] Schema summary:\n{summary!r}")

        # Split schema into table DDLS and build dependency graph
        tables = split_schema_to_tables(schema_text)
        print(f"[DEBUG] Extracted {len(tables)} table DDL(s)")

        graph, table_map = build_dependency_graph(tables)
        print(f"[DEBUG] Dependency graph tables: {list(graph.keys())}")

        # Generate SQL fixes for each table based on summary
        sql_fix = ""
        for table_name, ddl in table_map.items():
            fix_prompt = (
                f"You are a SQL Server expert. Based on the following table schema and issues summary:\n\n"
                f"Table: {table_name}\n\n"
                f"DDL:\n{ddl}\n\n"
                f"Issues summary:\n{summary}\n\n"
                "Suggest only T-SQL statements to update the above table to fix the issues.\n"
                "- Do NOT create or drop tables.\n"
                "- Only ALTER existing tables (ADD/DROP/ALTER COLUMN, ADD CONSTRAINT, etc).\n"
                "- Return ONLY valid T-SQL statements. DO NOT include ```sql, ``` or any markdown formatting.\n"
                "- No explanations or extra text—just plain SQL.\n"
            )
            partial_fix = call_llm_single(fix_prompt)
            if partial_fix:
                sql_fix += partial_fix.strip() + "\nGO\n"

        sql_fix = sql_fix.strip()
        print(f"[DEBUG] Generated SQL fix statements:\n{sql_fix}")

        # Combine original schema + fixes
        full_fixed_schema = schema_text + ("\n\n" + sql_fix if sql_fix else "")
        tables = split_schema_to_tables(full_fixed_schema)
        graph, table_map = build_dependency_graph(tables)

        # Chunk tables for ERD generation
        schema_chunks = chunk_tables(graph, table_map)

        all_nodes, all_edges = [], []
        example_json = '{"nodes":[{"id":"Employees","label":"Employees","type":"table","columns":[{"name":"EID","type":"int","isPrimaryKey":true,"isForeignKey":false,"nullable":false},{"name":"DeptID","type":"int","isPrimaryKey":false,"isForeignKey":true,"nullable":false}]}],"edges":[{"id":"FK_Employees_DeptID","label":"FK_Employees_DeptID","source":"Employees","sourceHandle":"DeptID","target":"Departments","targetHandle":"DeptID"}]}'
        for i, chunk in enumerate(schema_chunks):
            erd_prompt = f"""
You are given the following SQL Server schema definition for ONE TABLE ONLY:

{chunk}
{sql_fix}

Your task is to generate a **strictly valid JSON object** representing the ER diagram for this table, using the React Flow format.

### ✅ Output Format (DO NOT CHANGE):
Return a single JSON object with **exactly two keys**: "nodes" and "edges".

1. "nodes" must contain a list with ONE object:
   - "id": table name (string)
   - "label": same as table name (string)
   - "type": always "table"
   - "columns": list of column objects. Each column object must contain:
     - "name": column name (string)
     - "type": data type (string)
     - "isPrimaryKey": true or false
     - "isForeignKey": true or false
     - "nullable": true or false

2. "edges" must contain all foreign key relationships where this table is the source.
   Each edge object must include:
     - "source": this table name
     - "sourceHandle": the foreign key column name in this table
     - "target": the referenced table name
     - "targetHandle": the referenced column name

### ⚠️ RULES (YOU MUST FOLLOW):
- Output MUST be a single-line raw JSON string (no markdown, no explanation, no code block).
- Only include this table — DO NOT invent or add unrelated tables.
- If this table has no foreign keys, "edges" should be an empty list.
- Never wrap the response in triple backticks, markdown, or explanation.
- Never output nulls — always use booleans and strings explicitly.

### ✅ Example:
{example_json}

### Now do the same for this table below:
"""

            raw_erd = call_llm_single(erd_prompt)
            print(f"[DEBUG] LLM response for chunk {i}:\n{raw_erd!r}")  
            if not raw_erd:
                print(f"[DEBUG] LLM returned empty ERD for chunk {i}")
                continue
            print(f"[DEBUG] Raw ERD for chunk {i}:\n{raw_erd}")
            try:
                cleaned = clean_json_string(raw_erd)
                parsed = parse_erd_json(cleaned)

                nodes = [normalize_erd_node(n) for n in parsed.get("nodes", [])]
                edges = parsed.get("edges", [])

                print(f"[DEBUG] Chunk {i} → {len(nodes)} nodes, {len(edges)} edges")

                all_nodes.extend(nodes)
                all_edges.extend(edges)

            except Exception as e:
                print(f"[ERROR] Failed to parse ERD JSON for chunk {i}: {e}")
                continue

        # Remove duplicates (by id)
        unique_nodes = {n["id"]: n for n in all_nodes}
        unique_edges_set = set()
        unique_edges = []
        for e in all_edges:
            key = (e.get("source"), e.get("target"), e.get("label"))
            if key not in unique_edges_set:
                unique_edges_set.add(key)
                unique_edges.append(e)

        final_erd_json = json.dumps({
            "nodes": list(unique_nodes.values()),
            "edges": unique_edges
        }, separators=(",", ":"))

        return {
            "success": True,
            "textual_summary": summary,
            "sql_fix": sql_fix,
            "reactflow_erd": final_erd_json
        }

    except Exception as e:
        print(f"[ERROR] suggest_schema_improvements: {e}")
        return {"success": False, "message": str(e)}

# ------------------------- SQL Execution -------------------------

def apply_schema_fix(database_name: str, sql_statements: str) -> Dict:
    try:
        # Remove all ```sql ... ``` or ``` ... ``` code blocks
        # This regex removes any block starting with ``` optionally followed by "sql", then any content, ending with ```
        cleaned_sql = re.sub(r"```(?:sql)?\s*([\s\S]*?)```", r"\1", sql_statements, flags=re.IGNORECASE)

        # Strip again to remove leading/trailing spaces
        cleaned_sql = cleaned_sql.strip()

        # Split on GO (case-insensitive), ignoring empty splits
        statements = [stmt.strip() for stmt in re.split(r"\bGO\b", cleaned_sql, flags=re.IGNORECASE) if stmt.strip()]

        for idx, stmt in enumerate(statements, 1):
            print(f"[SQL Execution {idx}]: {stmt}")
            execute_query(stmt, database_name)

        return {"success": True, "message": f"Executed {len(statements)} SQL statements."}

    except Exception as e:
        print(f"[ERROR] SQL execution failed: {e}")
        return {"success": False, "message": str(e)}

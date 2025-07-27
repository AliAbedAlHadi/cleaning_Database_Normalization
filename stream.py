import streamlit as st
import requests
import streamlit_mermaid as st_mermaid

API_BASE = "http://127.0.0.1:8000"

# --- Page Setup ---
st.set_page_config(page_title="RAG AI Data Analysis", page_icon="📊", layout="wide")

# --- Custom CSS ---
st.markdown("""
    <style>
    .stApp {
        background: linear-gradient(135deg, #0f0f0f, #1c1c3c);
        color: #4fc3f7;
        font-family: 'Segoe UI', sans-serif;
    }
    .main-title {
        font-size: 2.8em;
        font-weight: bold;
        color: #4fc3f7;
        margin-bottom: 0.5em;
        text-shadow: 1px 1px 4px #000;
    }
    .stButton > button {
        background: linear-gradient(90deg, #4fc3f7, #0288d1);
        color: white;
        font-weight: bold;
        border: none;
        border-radius: 10px;
        padding: 0.6em 1.8em;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
        transition: 0.3s ease;
    }
    .stButton > button:hover {
        background: linear-gradient(90deg, #0288d1, #03a9f4);
        transform: scale(1.03);
        box-shadow: 0 0 20px #4fc3f7aa;
    }
    section[data-testid="st.text_area"] textarea,
    section[data-testid="stTextInput"] input {
        background-color: #1e1e2f !important;
        color: #ffffff !important;
        font-size: 16px !important;
        border-radius: 10px !important;
        border: 1px solid #444 !important;
        padding: 12px !important;
    }
    .stCodeBlock {
        background-color: #1e1e2f !important;
        color: #ffffff !important;
        font-family: monospace !important;
        border-radius: 10px !important;
    }
    .stMarkdown h2 {
        color: #4fc3f7;
        margin-top: 1.5em;
    }
    section[data-testid="stSidebar"] {
        background-color: #141414;
        color: white;
        border-right: 1px solid #333;
    }
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] .css-1wa3eu0-placeholder,
    section[data-testid="stSidebar"] .css-1uccc91-singleValue {
        color: #ffffff !important;
        font-weight: bold;
    }
    .stAlert[data-baseweb="toast"] {
        background: #263238 !important;
    }
    </style>
""", unsafe_allow_html=True)

# --- Title ---
st.markdown('<div class="main-title">RAG AI Data Analysis</div>', unsafe_allow_html=True)

# --- Sidebar: Database Selection ---
st.sidebar.header("Database Selection")
db_resp = requests.get(f"{API_BASE}/databases")
dbs = db_resp.json() if db_resp.ok else []
selected_db = st.sidebar.selectbox("Select Database", [""] + dbs)

# --- Initialize session state ---
if "prev_db" not in st.session_state:
    st.session_state.prev_db = None
if "schema_embedded" not in st.session_state:
    st.session_state.schema_embedded = {}
if "analyzed_tables" not in st.session_state:
    st.session_state.analyzed_tables = []

# Reset analysis if database changed
if selected_db != st.session_state.prev_db:
    st.session_state.analyzed_tables = []
    st.session_state.prev_db = selected_db

if selected_db:
    # Embed schema once per DB
    if not st.session_state.schema_embedded.get(selected_db, False):
        with st.spinner("Embedding schema if not already done..."):
            embed_resp = requests.post(f"{API_BASE}/embed-schema/{selected_db}")
            if embed_resp.ok and embed_resp.json().get("success"):
                st.sidebar.success(f"✅ Schema embedded for {selected_db}")
                st.session_state.schema_embedded[selected_db] = True
            else:
                st.sidebar.warning("⚠️ Schema may already be embedded or embedding failed.")

    # --- Tabs ---
    tab1, tab2, tab3 = st.tabs(["🧹 Table Fixing", "🧬 Schema Normalization", "🧠 Full Schema Optimization"])

    # --- Tab 1: Table Fixing ---
    with tab1:
        st.info("Click once to analyze all tables and review/fix them below.")
        user_question = st.text_input("Optional: What would you like to know from this database?", key="user_question")

        if st.button("🚀 Start Table Analysis", key="start_analysis"):
            st.session_state.analyzed_tables = []
            with st.spinner("Analyzing all tables..."):
                while True:
                    params = {"current_table": st.session_state.analyzed_tables[-1]["table_name"]} if st.session_state.analyzed_tables else {}
                    resp = requests.get(f"{API_BASE}/next-table/{selected_db}", params=params)
                    table_data = resp.json() if resp.ok else None

                    if not table_data or table_data.get("done"):
                        break

                    table_name = table_data["table_name"]
                    analysis_resp = requests.post(f"{API_BASE}/analyze-table", json={
                        "database": selected_db,
                        "table_name": table_name,
                        "user_question": user_question
                    })

                    if analysis_resp.ok:
                        fix_suggestion = analysis_resp.json().get("analysis", "").strip()
                        st.session_state.analyzed_tables.append({
                            "table_name": table_name,
                            "fix_suggestion": fix_suggestion,
                            "note": ""
                        })

            st.success("✅ All tables analyzed. See below.")

        # Show analysis results with notes and apply buttons
        for idx, entry in enumerate(st.session_state.analyzed_tables):
            with st.expander(f"🧪 Table: {entry['table_name']}", expanded=False):
                st.subheader(f"💡 Suggested Fix for `{entry['table_name']}`")
                st.text_area(f"Fix Description {idx}", value=entry["fix_suggestion"], height=200, disabled=True)
                note_key = f"note_{entry['table_name']}"
                note = st.text_input(f"📝 Your Note (optional) {idx}", value=entry.get("note", ""), key=note_key)
                st.session_state.analyzed_tables[idx]["note"] = note

                if st.button(f"✅ Apply Fix to {entry['table_name']}", key=f"fix_{entry['table_name']}"):
                    with st.spinner("Applying fix..."):
                        fix_payload = {
                            "database": selected_db,
                            "table_name": entry["table_name"],
                            "fix_description": entry["fix_suggestion"]
                        }
                        apply_resp = requests.post(f"{API_BASE}/apply-fix", json=fix_payload)

                    if apply_resp.ok and apply_resp.json().get("success"):
                        st.success("✅ Fix applied successfully.")
                    else:
                        st.error("❌ Failed to apply fix.")

    # --- Tab 2: Schema Normalization ---
    with tab2:
        st.subheader("🔍 Analyze Full Schema for Normalization")

        if st.button("Run Schema Normalization Analysis", key="normalize_analysis"):
            with st.spinner("Analyzing schema..."):
                resp = requests.post(f"{API_BASE}/normalize/analyze", json={"database": selected_db})
                if resp.ok and resp.json().get("success"):
                    st.code(resp.json().get("sql_to_review"), language="sql")
                else:
                    st.error("❌ Failed to analyze schema.")

        normalization_sql = st.text_area("✍️ Paste Normalization SQL to Apply", max_chars=None, height=300, key="normalization_sql")
        if st.button("🚀 Apply Normalization SQL", key="apply_normalization") and normalization_sql.strip():
            with st.spinner("Applying normalization..."):
                payload = {"database": selected_db, "sql_statements": normalization_sql.strip()}
                resp = requests.post(f"{API_BASE}/normalize/apply", json=payload)
                if resp.ok and resp.json().get("success"):
                    st.success("✅ Normalization SQL applied successfully.")
                else:
                    st.error("❌ Failed to apply normalization SQL.")

    # --- Tab 3: Schema Suggestion + ER Diagram ---
    with tab3:
        st.subheader("🔎 Full Schema Analysis & Optimization")

        if st.button("📊 Analyze Schema with AI", key="analyze_schema_ai"):
            with st.spinner("Analyzing schema and generating improvements..."):
                try:
                    response = requests.post(f"{API_BASE}/schema/suggest", json={"database": selected_db})
                    data = response.json()
                except Exception as e:
                    st.error(f"❌ API Error: {e}")
                    st.stop()

            if not data.get("success"):
                st.error(f"❌ Failed: {data.get('message', 'Unknown error')}")
            else:
                st.success("✅ Schema analyzed successfully!")

                # Display summary
                st.markdown("### 📋 Suggested Improvements")
                st.text_area("Summary", data.get("textual_summary", ""), height=200, disabled=True, key="summary_area")

                # Display SQL Fix
                sql_fix = data.get("sql_fix", "")
                if sql_fix.strip():
                    st.markdown("### 🛠️ SQL Fix")
                    st.code(sql_fix, language="sql")

                    if st.button("🚀 Apply SQL Fix to Database", key="apply_sql_fix"):
                        with st.spinner("Applying fix..."):
                            apply_resp = requests.post(f"{API_BASE}/schema/apply-fix", json={
                                "database": selected_db,
                                "sql_statements": sql_fix
                            })
                            if apply_resp.ok and apply_resp.json().get("success"):
                                st.success("✅ Fix applied successfully.")
                            else:
                                st.error("❌ Failed to apply SQL fix.")

                # Display Mermaid Diagram
                mermaid_code = data.get("er_diagram_mermaid", "")

                # Cleanup Mermaid code: remove markdown fences if present
                mermaid_code = mermaid_code.strip()
                if mermaid_code.startswith("```") and mermaid_code.endswith("```"):
                    mermaid_code = "\n".join(mermaid_code.split("\n")[1:-1])

                if mermaid_code:
                    st.markdown("### 🧭 ER Diagram")
                    try:
                        st_mermaid.st_mermaid(mermaid_code)
                    except Exception as e:
                        st.error(f"❌ Mermaid rendering error: {e}")
                        st.code(mermaid_code, key="mermaid_raw_code")
                else:
                    st.warning("⚠️ No Mermaid diagram returned.")
else:
    st.warning("👈 Please select a database from the sidebar to begin.")

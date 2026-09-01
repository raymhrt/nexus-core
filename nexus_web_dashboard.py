import streamlit as st
import sqlite3
from pathlib import Path
import pandas as pd

st.set_page_config(page_title="QuantCode Nexus Dashboard", page_icon="🚀", layout="wide")

BASE_DIR = Path(r"C:\QuantCode\NexusCore")
DB_PATH = BASE_DIR / "nexus_economy.db"

def get_data():
    if not DB_PATH.exists():
        return None, None, None
    conn = sqlite3.connect(str(DB_PATH))
    agents = pd.read_sql("SELECT * FROM agent_registry", conn)
    leads = pd.read_sql("SELECT * FROM b2b_leads ORDER BY scraped_at DESC LIMIT 15", conn)
    ledger = pd.read_sql("SELECT * FROM revenue_ledger", conn)
    conn.close()
    return agents, leads, ledger

st.title("🚀 QuantCode Nexus — Production Economic Engine")
st.markdown("Live operational telemetry, agent performance tracking, and automated lead ingestion.")

agents, leads, ledger = get_data()

if agents is None:
    st.error(f"Database not found at {DB_PATH}. Ensure the core engine has initialized.")
else:
    col1, col2, col3 = st.columns(3)
    total_rev = ledger['amount'].sum() if not ledger.empty else 0.0
    total_txs = len(ledger)
    active_agents = len(agents)

    col1.metric("Cumulative Engine Revenue", f"${total_rev:,.2f} USD")
    col2.metric("Verified Transactions", total_txs)
    col3.metric("Registered Agents", active_agents)

    st.markdown("---")
    st.subheader("🤖 Active Agent Registry & Performance")
    st.dataframe(agents, width='stretch')

    st.markdown("---")
    st.subheader("🎯 Recently Harvested B2B Leads")
    st.dataframe(leads, width='stretch')
import json
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

DASHBOARD_DIR = Path(os.getenv("DASHBOARD_DIR", "/app/dashboard_data"))

st.set_page_config(
    page_title="🛵 Zomato Delivery Dashboard",
    page_icon="🍕",
    layout="wide",
)


def load_snapshot() -> dict | None:
    p = DASHBOARD_DIR / "latest_snapshot.json"
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def load_history() -> list[dict]:
    p = DASHBOARD_DIR / "history.jsonl"
    if not p.exists():
        return []
    records = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


# ── Sidebar ────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Settings")
    refresh_seconds = st.slider("Refresh interval (s)", 2, 30, 5)
    st.markdown("---")
    st.markdown("**Stack**")
    st.markdown("Kafka → Spark Streaming → Streamlit")
    st.markdown("**Dataset**: Zomato Delivery Analytics")

st.title("🛵 Zomato Delivery Operations — Live Dashboard")
st.caption(f"Reading from: `{DASHBOARD_DIR}`")


@st.fragment(run_every=refresh_seconds)
def live_dashboard():
    snapshot = load_snapshot()
    history  = load_history()

    if snapshot is None:
        st.warning("⏳ Waiting for Spark job output...")
        st.code(
            "docker exec -it spark-master /opt/spark/bin/spark-submit \\\n"
            "  --master spark://spark-master:7077 \\\n"
            "  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \\\n"
            "  /opt/zomato/jobs/streaming_job.py",
            language="bash",
        )
        return

    rows = snapshot.get("rows", [])
    df   = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["Weather_conditions", "count", "avg_delivery_min"])
    if not df.empty:
        df["count"]            = df["count"].astype(int)
        df["avg_delivery_min"] = df["avg_delivery_min"].astype(float)

    batch_id       = snapshot.get("batch_id", "-")
    updated_at     = snapshot.get("updated_at", "-")
    total          = int(df["count"].sum()) if not df.empty else 0
    orders_per_min = snapshot.get("orders_per_minute", "-")      # R1
    cur_avg_min    = snapshot.get("current_avg_delivery_min", "-")  # R2

    # ── KPI Cards (5 columns) ─────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🔢 Batch ID",               batch_id)
    c2.metric("🕐 Last Updated",            updated_at)
    c3.metric("📦 Total Orders",   f"{total:,}")
    c4.metric("⚡ Orders / Min",        f"{orders_per_min}")
    c5.metric("⏱️ Avg Delivery Min",   f"{cur_avg_min} min")

    st.divider()

    # ── Charts ────────────────────────────────────────────────
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("☁️ Orders by Weather Condition")
        if not df.empty:
            fig1 = px.bar(
                df.sort_values("count", ascending=False),
                x="Weather_conditions", y="count",
                color="count", color_continuous_scale="Blues",
                template="plotly_dark",
            )
            fig1.update_layout(showlegend=False, margin=dict(t=20, b=20))
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("Waiting for data...")

    with col_b:
        st.subheader("⏱️ Avg Delivery Time by Weather")
        if not df.empty:
            fig2 = px.bar(
                df.sort_values("avg_delivery_min", ascending=False),
                x="Weather_conditions", y="avg_delivery_min",
                color="avg_delivery_min", color_continuous_scale="RdYlGn_r",
                template="plotly_dark",
                labels={"avg_delivery_min": "Avg Time (min)"},
            )
            fig2.update_layout(showlegend=False, margin=dict(t=20, b=20))
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Waiting for data...")

    # ── History Chart ─────────────────────────────────────────
    if history:
        st.subheader("📈 Total Orders per Batch (Stream History)")
        hdf = pd.DataFrame(history)[["batch_id", "total_orders", "orders_per_min", "avg_delivery_min"]]
        col_h1, col_h2 = st.columns(2)
        with col_h1:
            st.caption("Orders per Batch")
            st.line_chart(hdf.set_index("batch_id")["total_orders"])
        with col_h2:
            st.caption("Avg Delivery Time Trend (min)")
            st.line_chart(hdf.set_index("batch_id")["avg_delivery_min"])

    # ── Raw preview ───────────────────────────────────────────
    with st.expander("🔍 Current Snapshot Rows"):
        st.dataframe(df, use_container_width=True) if not df.empty else st.info("No rows yet.")
    with st.expander("🗃️ Batch History"):
        if history:
            st.dataframe(pd.DataFrame(history).sort_values("batch_id", ascending=False),
                         use_container_width=True)


live_dashboard()
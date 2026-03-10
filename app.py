"""
Weather ETL Pipeline — Web UI
=============================
Streamlit-based dashboard to run and monitor the ETL pipeline.
"""

import io
import logging
import sys

import pandas as pd
import streamlit as st

from config import CITIES, DB_CONFIG, PIPELINE_CONFIG
from etl_pipeline import run_pipeline
from loader import WeatherLoader
from logger import setup_logger

# Columns editable in the UI (others are read-only)
EDITABLE_COLUMNS = [
    "temp_c", "temp_f", "humidity_pct", "precipitation_mm", "rain_mm", "snowfall_cm",
    "wind_speed_ms", "wind_speed_kph", "pressure_hpa", "cloud_cover_pct",
    "visibility_m", "uv_index", "weather_desc",
]

# Page config (must be first Streamlit call)
st.set_page_config(
    page_title="Weather ETL Pipeline",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for a cleaner look
st.markdown("""
<style>
    .main-header {
        font-size: 2rem;
        font-weight: 700;
        color: #1E3A5F;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        color: #64748b;
        font-size: 0.95rem;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
        padding: 1rem 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #0284c7;
        margin-bottom: 0.5rem;
    }
    .status-success {
        color: #059669;
        font-weight: 600;
    }
    .status-partial {
        color: #d97706;
        font-weight: 600;
    }
    .status-failure {
        color: #dc2626;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


def run_etl_with_capture(cities: list, dry_run: bool):
    """Run pipeline and capture log output."""
    log_capture = io.StringIO()
    handler = logging.StreamHandler(log_capture)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s  %(levelname)-8s  %(name)-20s  %(message)s")
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.addHandler(handler)

    try:
        summary = run_pipeline(cities, dry_run=dry_run)
        return summary, log_capture.getvalue()
    finally:
        root.removeHandler(handler)


def render_run_etl_tab():
    """Run ETL pipeline tab."""
    city_names = [c["name"] for c in CITIES]
    selected_cities = st.multiselect(
        "Cities to process",
        options=city_names,
        default=city_names,
        help="Select cities. Empty = run all.",
    )
    dry_run = st.checkbox("Dry run (skip DB load)", value=False)

    run_clicked = st.button("▶️ Run ETL Pipeline", type="primary")

    if run_clicked:
        cities_to_run = [c for c in CITIES if c["name"] in selected_cities] if selected_cities else CITIES
        if not cities_to_run:
            st.warning("Select at least one city.")
            return
        mode = "DRY RUN" if dry_run else "FULL ETL"
        st.info(f"Running **{mode}** for {len(cities_to_run)} city/cities")
        with st.spinner("Running pipeline..."):
            summary, log_output = run_etl_with_capture(cities_to_run, dry_run)

        status = summary["status"]
        status_class = "status-success" if status == "SUCCESS" else "status-partial"
        st.markdown(f'**Status:** <span class="{status_class}">{status}</span>', unsafe_allow_html=True)
        cols = st.columns(4)
        cols[0].metric("Cities processed", f"{summary['cities_processed']}/{len(cities_to_run)}")
        cols[1].metric("Records extracted", summary["records_extracted"])
        cols[2].metric("Records transformed", summary["records_transformed"])
        cols[3].metric("Records loaded", summary["records_loaded"])
        if summary["errors"]:
            st.error("Errors occurred:")
            for err in summary["errors"]:
                st.code(f"{err['city']}: {err['error']}", language=None)
        with st.expander("📋 View log output", expanded=False):
            st.code(log_output, language="text")
    else:
        st.caption("Configured cities:")
        for c in CITIES:
            st.write(f"• **{c['name']}** — {c['lat']}, {c['lon']} ({c['timezone']})")


def _safe_eq(ov, nv):
    """Compare values, handling NaN and float precision."""
    if pd.isna(ov) and pd.isna(nv):
        return True
    if pd.isna(ov) or pd.isna(nv):
        return False
    try:
        if isinstance(ov, (int, float)) and isinstance(nv, (int, float)):
            return abs(float(ov) - float(nv)) < 1e-9
    except (TypeError, ValueError):
        pass
    return ov == nv


def _to_py(val):
    """Convert numpy/pandas types to native Python for MySQL."""
    if pd.isna(val):
        return None
    if hasattr(val, "item"):
        return val.item()
    return val


def render_edit_data_tab():
    """Edit data tab - fetch, edit, save to MySQL."""
    loader = WeatherLoader(DB_CONFIG)

    st.caption("**Step 1:** Filter and load data")
    city_filter = st.selectbox(
        "City",
        options=["All"] + [c["name"] for c in CITIES],
        index=0,
    )
    days_back = st.number_input("Days to look back", min_value=1, max_value=90, value=7)
    limit = st.slider("Max rows to load", min_value=50, max_value=500, value=200)

    if st.button("🔍 Load data"):
        city = None if city_filter == "All" else city_filter
        try:
            records = loader.fetch_records(city=city, limit=limit, days_back=days_back)
            st.session_state["edit_records"] = records
            st.session_state["edit_original"] = [dict(r) for r in records]
            st.session_state["edit_load_id"] = st.session_state.get("edit_load_id", 0) + 1
        except Exception as e:
            st.error(f"Failed to load: {e}")

    if "edit_records" not in st.session_state:
        st.info("**Step 1:** Click **Load data** to fetch records from the database.")
        return

    df = pd.DataFrame(st.session_state["edit_records"])
    if df.empty:
        st.warning("No records found. Run the ETL pipeline first or adjust filters.")
        return

    # Column config: only id/city/observed_at/ingested_at are read-only; rest editable
    col_config = {}
    for col in df.columns:
        if col in ("id", "city", "observed_at", "ingested_at"):
            col_config[col] = st.column_config.Column(disabled=True)
        elif col == "weather_desc":
            col_config[col] = st.column_config.TextColumn(col, width="medium")

    st.caption("**Step 2:** Click a cell and type to edit (temp_c, humidity_pct, weather_desc, etc.)")
    editor_key = f"data_editor_{st.session_state.get('edit_load_id', 0)}"
    edited = st.data_editor(
        df,
        use_container_width=True,
        num_rows="fixed",
        column_config=col_config,
        key=editor_key,
    )

    st.caption("**Step 3:** Click Save to write changes to MySQL")
    if st.button("💾 Save changes to database"):
        original = pd.DataFrame(st.session_state["edit_original"])
        changed = 0
        errors = []

        for i in range(min(len(original), len(edited))):
            orig_row = original.iloc[i]
            edit_row = edited.iloc[i]
            rec_id = int(orig_row["id"])
            updates = {}

            for col in EDITABLE_COLUMNS:
                if col not in edit_row.index:
                    continue
                ov, nv = orig_row.get(col), edit_row[col]
                if not _safe_eq(ov, nv):
                    if pd.isna(nv):
                        updates[col] = None
                    elif col == "humidity_pct":
                        updates[col] = int(_to_py(nv))
                    else:
                        updates[col] = _to_py(nv)

            if updates:
                try:
                    loader.update_record(rec_id, updates)
                    changed += 1
                except Exception as e:
                    errors.append(f"Row id={rec_id}: {e}")

        if changed:
            st.success(f"✓ Saved {changed} row(s) to MySQL.")
            saved = edited.to_dict("records")
            st.session_state["edit_records"] = saved
            st.session_state["edit_original"] = [dict(r) for r in saved]
        if errors:
            st.error("Some updates failed:")
            for err in errors:
                st.code(err, language=None)
        if not changed and not errors:
            st.info("No changes detected. Edit a cell first, then click Save.")


def main():
    st.markdown('<p class="main-header">🌦️ Weather ETL Pipeline</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="sub-header">Extract weather data from Open-Meteo → Transform → Load into MySQL</p>',
        unsafe_allow_html=True,
    )
    setup_logger()

    with st.sidebar:
        st.header("⚙️ Navigation")
        st.divider()
        st.caption("Pipeline config")
        st.code(
            f"Batch size: {PIPELINE_CONFIG['batch_size']}\n"
            f"API delay: {PIPELINE_CONFIG['api_delay_seconds']}s",
            language=None,
        )
        st.divider()
        st.caption("Database")
        st.code(
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}\n{DB_CONFIG['database']}",
            language=None,
        )

    tab1, tab2 = st.tabs(["▶️ Run ETL", "✏️ Edit Data"])
    with tab1:
        render_run_etl_tab()
    with tab2:
        render_edit_data_tab()


if __name__ == "__main__":
    main()

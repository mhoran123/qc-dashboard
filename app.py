import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np
import os


engine = create_engine(DB_URL)


# UI
st.set_page_config(layout="wide")
st.title("Spin QC Database Dashboard")

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    start_date = st.date_input("Start Date", pd.to_datetime("2025-01-01"))
    end_date = st.date_input("End Date", pd.to_datetime("2025-05-15"))
    
    # Updated with all products and locations
    all_products = ["203", "303", "Q203", "FF104", "Q104", "DW 21", "DW13", "FF203", 
                   "103", "801", "204", "26743BIO", "304", "BC 803", "402", "26744BIO", "104"]
    products = st.multiselect("Products", all_products, default=["203"])
    
    locations = st.multiselect("Locations", ["Newark", "Chestertown"], default=["Newark"])
    
    # Updated fill lines based on location info
    fill_options = ["Fill-1", "Fill-2", "Fill-3", "Fill-4", "Fill-5", "Fill-6", "Fill-7"]
    fills = st.multiselect("Fill Lines", fill_options, 
                          default=["Fill-1", "Fill-2", "Fill-3", "Fill-4", "Fill-5", "Fill-6", "Fill-7"])

# --- Load data ---
@st.cache_data(show_spinner=True)
def load_data(_engine, start_date, end_date, locations, products, fills):
    if not locations or not products:
        st.error("Please select at least one product and one location.")
        st.stop()

    try:
        with _engine.connect() as conn:
            # Step 1: Get product IDs
            products_query = """
                SELECT product_id, name as disk_series 
                FROM products 
                WHERE name = ANY(%(products)s)
            """
            products_df = pd.read_sql(products_query, conn, params={'products': products})
            
            if products_df.empty:
                st.error("No products found matching the selected criteria.")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            
            product_ids = products_df['product_id'].tolist()
            
            # Step 2: Get location IDs  
            locations_query = """
                SELECT location_id, location_name 
                FROM locations 
                WHERE location_name = ANY(%(locations)s)
            """
            locations_df = pd.read_sql(locations_query, conn, params={'locations': locations})
            
            if locations_df.empty:
                st.error("No locations found matching the selected criteria.")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                
            location_ids = locations_df['location_id'].tolist()
            
            # Step 3: Get fill IDs from fill_lines table
            fills_query = """
                SELECT * FROM fill_lines 
                WHERE display = ANY(%(fills)s)
            """
            fills_df = pd.read_sql(fills_query, conn, params={'fills': fills})
            
            if fills_df.empty:
                st.error("No fill lines found matching the selected criteria.")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            
            # Find the ID column name for fills
            if 'fill_line_id' in fills_df.columns:
                fill_ids = fills_df['fill_line_id'].tolist()
            elif 'id' in fills_df.columns:
                fill_ids = fills_df['id'].tolist()
            else:
                # Use the first numeric column
                numeric_cols = fills_df.select_dtypes(include=['int64', 'int32']).columns
                if len(numeric_cols) > 0:
                    fill_ids = fills_df[numeric_cols[0]].tolist()
                else:
                    st.error("Could not find integer ID column in fill_lines table")
                    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            
            # Step 4: Get samples (excluding test samples)
            samples_query = """
                SELECT * FROM sample_set
                WHERE collected >= %(start_date)s 
                AND collected < %(end_date)s
                AND product_id = ANY(%(product_ids)s)
                AND location_id = ANY(%(location_ids)s)
                AND fill_id = ANY(%(fill_ids)s)
                AND is_test = false
            """
            samples_df = pd.read_sql(samples_query, conn, params={
                'start_date': start_date,
                'end_date': end_date,
                'product_ids': product_ids,
                'location_ids': location_ids,
                'fill_ids': fill_ids
            })
            
            if samples_df.empty:
                st.warning("No sample data found for the selected filters.")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            
            sample_set_ids = samples_df['sample_set_id'].tolist()
            
            # Step 5: Get approvals for those samples
            approvals_query = """
                SELECT * FROM approvals 
                WHERE sample_set_id = ANY(%(sample_set_ids)s)
            """
            approvals_df = pd.read_sql(approvals_query, conn, params={'sample_set_ids': sample_set_ids})
            
            # Step 6: Get latest approval for each sample
            if not approvals_df.empty:
                latest_approvals = (approvals_df
                                  .sort_values('timestamp')
                                  .groupby('sample_set_id')
                                  .last()
                                  .reset_index())
                approval_ids = latest_approvals['approval_id'].tolist()
            else:
                latest_approvals = pd.DataFrame()
                approval_ids = []
            
            # Step 7: Get sample defects and reagent fails
            sample_defects_df = pd.DataFrame()
            reagent_fails_df = pd.DataFrame()
            specs_df = pd.DataFrame()
            
            if approval_ids:
                # Sample defects
                sample_defects_query = """
                    SELECT * FROM sample_defects 
                    WHERE approval_id = ANY(%(approval_ids)s)
                """
                sample_defects_df = pd.read_sql(sample_defects_query, conn, params={'approval_ids': approval_ids})
                
                # Reagent fails
                reagent_fails_query = """
                    SELECT * FROM reagent_fails 
                    WHERE approval_id = ANY(%(approval_ids)s)
                """
                reagent_fails_df = pd.read_sql(reagent_fails_query, conn, params={'approval_ids': approval_ids})
                
                # Get specs and standards for reagent fails
                if not reagent_fails_df.empty:
                    specs_query = """
                        SELECT s.*, st.name as standard_name 
                        FROM specs s
                        LEFT JOIN standards st ON s.standard_id = st.standard_id
                        WHERE s.product_id = ANY(%(product_ids)s)
                    """
                    specs_df = pd.read_sql(specs_query, conn, params={'product_ids': product_ids})
            
            return samples_df, latest_approvals, products_df, locations_df, sample_defects_df, reagent_fails_df, specs_df
            
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# Load the data
samples_df, approvals_df, products_df, locations_df, sample_defects_df, reagent_fails_df, specs_df = load_data(
    engine, start_date, end_date, locations, products, fills
)

if samples_df.empty:
    st.stop()

# --- Merge data ---
if not approvals_df.empty:
    # Merge samples with latest approvals
    samples_df = samples_df.merge(approvals_df, on="sample_set_id", how="left", suffixes=("", "_approval"))

# Merge with products and locations for display names
samples_df = samples_df.merge(products_df, on="product_id", how="left")
samples_df = samples_df.merge(locations_df, on="location_id", how="left")

# Create fill display mapping and merge
fills_query = """
    SELECT * FROM fill_lines 
    WHERE display = ANY(%(fills)s)
"""
with engine.connect() as conn:
    fills_display_df = pd.read_sql(fills_query, conn, params={'fills': fills})

# Find the ID column name for fills
if 'fill_line_id' in fills_display_df.columns:
    id_col = 'fill_line_id'
elif 'id' in fills_display_df.columns:
    id_col = 'id'
else:
    numeric_cols = fills_display_df.select_dtypes(include=['int64', 'int32']).columns
    id_col = numeric_cols[0] if len(numeric_cols) > 0 else 'id'

# Merge fill display names
samples_df = samples_df.merge(
    fills_display_df[[id_col, 'display']], 
    left_on='fill_id', 
    right_on=id_col, 
    how='left'
).rename(columns={'display': 'fill_display'})

# Convert timestamps and create time groupings
samples_df["collected"] = pd.to_datetime(samples_df["collected"])
samples_df["timespan"] = samples_df["collected"].dt.to_period("M").dt.to_timestamp()

# --- Process Sample Defects ---
sample_defect_codes = [
    "Other", "BB Misplacement", "BB Missing", "Bead Incorrect", 
    "Bead Misplacement", "Bead Missing", "Disk Body Failure", "Disk Bubbles", 
    "Disk Foreign Matter", "Disk Leaking", "Disk Lid Failure", "Oven Malfunction", 
    "Oven Temperature", "Reagent Adjustment Failure", "Reagent Contamination Sprays Drips", 
    "Reagent CrossedLines", "Reagent Discoloration", "Reagent Expired", 
    "Reagent Fill Machine Weight", "Reagent Incorrect", "Reagent Missing", 
    "Room GPP", "Room Power Outage"
]

def assign_sample_defect_description(fail_type):
    if pd.isna(fail_type):
        return None
    try:
        return sample_defect_codes[int(fail_type)]
    except (IndexError, ValueError):
        return "Unknown"

# Process sample defects
if not sample_defects_df.empty:
    sample_defects_df['sample_defect'] = sample_defects_df['type'].apply(assign_sample_defect_description)

# Merge reagent fails with specs
if not reagent_fails_df.empty and not specs_df.empty:
    reagent_fails_df = reagent_fails_df.merge(specs_df, left_on='spec_id', right_on='id', how='left')

# --- Calculate Failure Analysis ---
# Filter to only approved/failed samples (state 1 = passed, state 3 = failed)
analyzed_samples = samples_df[samples_df["state"].isin([1, 3])].copy()

# --- Main Metrics ---
st.subheader("QC Summary")
total_samples = len(analyzed_samples)
total_passed = len(analyzed_samples[analyzed_samples["state"] == 1])
total_failed = len(analyzed_samples[analyzed_samples["state"] == 3])
fail_rate = (total_failed / total_samples * 100) if total_samples > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Samples", total_samples)
col2.metric("Passed", total_passed)
col3.metric("Failed", total_failed)
col4.metric("Fail Rate", f"{fail_rate:.1f}%")

# --- Fail rate over time ---
st.subheader("Fail Rate by Month")
if not analyzed_samples.empty:
    monthly_stats = (analyzed_samples
                    .groupby(['timespan', 'state'])
                    .size()
                    .unstack(fill_value=0)
                    .reset_index())
    
    # Calculate fail rate
    passed_col = 1 if 1 in monthly_stats.columns else None
    failed_col = 3 if 3 in monthly_stats.columns else None
    
    if passed_col is not None and failed_col is not None:
        monthly_stats["total"] = monthly_stats[passed_col] + monthly_stats[failed_col]
        monthly_stats["fail_rate"] = (monthly_stats[failed_col] / monthly_stats["total"]) * 100
    elif failed_col is not None:
        monthly_stats["fail_rate"] = 100.0  # All failed
    else:
        monthly_stats["fail_rate"] = 0.0  # All passed

    fig = px.line(
        monthly_stats,
        x="timespan",
        y="fail_rate",
        markers=True,
        title="Monthly Fail Rate (%)",
        labels={"fail_rate": "Fail Rate (%)", "timespan": "Month"}
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Failures by Fill Line ---
st.subheader("Failures by Fill Line")
if not analyzed_samples.empty:
    fill_analysis = (analyzed_samples
                    .groupby(['fill_display', 'state'])
                    .size()
                    .unstack(fill_value=0)
                    .reset_index())
    
    if 3 in fill_analysis.columns:  # Has failures
        fill_analysis = fill_analysis.rename(columns={3: 'failures'})
        
        fig2 = px.bar(
            fill_analysis,
            x="fill_display",
            y="failures",
            title="Failures by Fill Line",
            labels={"fill_display": "Fill Line", "failures": "Number of Failures"}
        )
        fig2.update_xaxes(tickangle=45)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No failures found in the selected data.")

# --- Reagent Failure Analysis ---
st.subheader("Reagent Failure Analysis")
if not reagent_fails_df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Failures by Reagent Type**")
        if 'reagent' in reagent_fails_df.columns:
            reagent_counts = reagent_fails_df['reagent'].value_counts()
            if not reagent_counts.empty:
                fig3 = px.bar(
                    x=reagent_counts.index,
                    y=reagent_counts.values,
                    labels={'x': 'Reagent', 'y': 'Failure Count'},
                    title="Reagent Failures"
                )
                fig3.update_xaxes(tickangle=45)
                st.plotly_chart(fig3, use_container_width=True)
    
    with col2:
        st.write("**Failures by Standard**")
        if 'standard_name' in reagent_fails_df.columns:
            standard_counts = reagent_fails_df['standard_name'].value_counts()
            if not standard_counts.empty:
                fig4 = px.bar(
                    x=standard_counts.index,
                    y=standard_counts.values,
                    labels={'x': 'Standard', 'y': 'Failure Count'},
                    title="Standard Failures"
                )
                fig4.update_xaxes(tickangle=45)
                st.plotly_chart(fig4, use_container_width=True)
else:
    st.info("No reagent failure data found.")

# --- Sample Defect Analysis ---
st.subheader("Sample Defect Analysis")
if not sample_defects_df.empty:
    defect_counts = sample_defects_df['sample_defect'].value_counts()
    
    fig5 = px.pie(
        values=defect_counts.values,
        names=defect_counts.index,
        title="Sample Defects Distribution"
    )
    st.plotly_chart(fig5, use_container_width=True)
    
    # Defect details table
    st.write("**Sample Defect Details**")
    defect_summary = sample_defects_df.groupby(['sample_defect']).size().reset_index(name='count')
    defect_summary = defect_summary.sort_values('count', ascending=False)
    st.dataframe(defect_summary, use_container_width=True)
else:
    st.info("No sample defect data found.")

# --- Location vs Fill Line Analysis ---
st.subheader("Location and Fill Line Analysis")
location_fill_analysis = (analyzed_samples
                         .groupby(['location_name', 'fill_display', 'state'])
                         .size()
                         .unstack(fill_value=0)
                         .reset_index())

if not location_fill_analysis.empty:
    # Calculate fail rates by location and fill line
    if 1 in location_fill_analysis.columns and 3 in location_fill_analysis.columns:
        location_fill_analysis['total'] = location_fill_analysis[1] + location_fill_analysis[3]
        location_fill_analysis['fail_rate'] = (location_fill_analysis[3] / location_fill_analysis['total']) * 100
        
        fig6 = px.bar(
            location_fill_analysis,
            x='fill_display',
            y='fail_rate',
            color='location_name',
            title="Fail Rate by Fill Line and Location",
            labels={'fail_rate': 'Fail Rate (%)', 'fill_display': 'Fill Line'},
            barmode='group'
        )
        fig6.update_xaxes(tickangle=45)
        st.plotly_chart(fig6, use_container_width=True)

# --- Sample Details Table ---
st.subheader("Sample Details")
display_columns = ["sample_set_id", "collected", "state", "fill_display", "location_name", "disk_series"]
available_columns = [col for col in display_columns if col in samples_df.columns]

# Add state description mapping
state_mapping = {1: "Passed", 3: "Failed"}
if 'state' in samples_df.columns:
    samples_df['status'] = samples_df['state'].map(state_mapping).fillna("Unknown")
    if 'status' not in available_columns:
        available_columns = [col if col != 'state' else 'status' for col in available_columns]
        if 'state' in available_columns:
            available_columns[available_columns.index('state')] = 'status'

if available_columns:
    display_df = samples_df[available_columns].head(100).copy()
    # Rename columns for better display
    column_renames = {
        'sample_set_id': 'Sample ID',
        'collected': 'Collection Date',
        'status': 'Status',
        'fill_display': 'Fill Line',
        'location_name': 'Location',
        'disk_series': 'Product'
    }
    display_df = display_df.rename(columns=column_renames)
    st.dataframe(display_df, use_container_width=True)
else:
    st.dataframe(samples_df.head(100), use_container_width=True)

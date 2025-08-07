# Spin QC Database Dashboard

A Streamlit web application for analyzing QC data from the Spin database.

## Features

- **QC Summary Metrics**: Total samples, pass/fail rates
- **Time Analysis**: Monthly fail rate trends
- **Fill Line Analysis**: Performance by fill line
- **Reagent Analysis**: Failures by reagent type and standard
- **Sample Defect Analysis**: Defect distribution and details
- **Location Analysis**: Performance by location and fill line
- **Sample Details**: Recent sample information

## Deployment

This dashboard is deployed on Streamlit Cloud for easy access.

## Local Development

To run locally:

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Data Sources

- PostgreSQL database with QC sample data
- Tables: sample_set, approvals, reagent_fails, sample_defects, specs, products, locations, fill_lines
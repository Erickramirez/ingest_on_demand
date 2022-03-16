#!/bin/sh
python -m ingest_on_demand.cli load-csv
python -m ingest_on_demand.cli execute-pg-task --event_name=create_schemas
python -m ingest_on_demand.cli execute-pg-task --event_name=create_tables
python -m ingest_on_demand.cli execute-pg-task --event_name=update_dimension_tables
python -m ingest_on_demand.cli execute-pg-task --event_name=update_fact_tables

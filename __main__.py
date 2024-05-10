from scripts.job import table_create_and_ingest, bronze_transformation
from utils.ingestion_utils import table_function_mapping
from utils.bronze_utils import selected_columns


def main():
    #table_create_and_ingest() => repeated for all data sources for each in table_sources_mapping
    environment = "dot_dev.db"
    
    for table in table_function_mapping:
        table_create_and_ingest(environment,table)
    
    for table_name in selected_columns:
        cols = selected_columns.get(table)
        if len(cols) == 0:
            pass
        else:
            bronze_transformation('dot_dev.db', table_name)


if __name__ == "__main__":
    main()
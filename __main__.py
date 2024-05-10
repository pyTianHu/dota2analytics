from scripts.job import table_create_and_ingest, bronze_transformation, bronze_to_silver_transformation
from utils.ingestion_utils import table_function_mapping
from utils.bronze_utils import bronze_selected_columns
from utils.silver_utils import silver_selected_columns
from utils.utils import logger


def main():
    #table_create_and_ingest() => repeated for all data sources for each in table_sources_mapping
    l = logger("End-to-End process is started", "main.py")
    l.new_or_existing_run()

    RAW_DB_DEV = "dot_dev.db"
    RAW_DB_PROD = "dot_raw_prod.db"

    BRONZE_DB_DEV = "dot_dev_bronze.db"
    BRONZE_DB_PROD = "dot_bronze_prod.db"

    SILVER_DB_DEV = "dot_dev_silver.db"
    SILVER_DB_PROD = "dot_silver_prod.db"
    
    #table_create_and_ingest(raw_db_name, table_name)
    for table_name in table_function_mapping:
        table_create_and_ingest(RAW_DB_DEV,table_name)
    
    for table_name in bronze_selected_columns:
        #bronze_transformation(raw_db_name, table_name, bronze_db_name)
        bronze_transformation(RAW_DB_DEV, table_name, BRONZE_DB_DEV)
    
    for table_name in silver_selected_columns:
        #bronze_to_silver_transformation(bronze_db_name, table_name, silver_db_name)
        bronze_to_silver_transformation(BRONZE_DB_DEV, table_name, SILVER_DB_DEV)

    # at the very end of the process call rename log function to the new execution will be added into a new log file.

if __name__ == "__main__":
    main()
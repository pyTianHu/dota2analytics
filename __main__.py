from scripts.job import table_create_and_ingest, bronze_transformation
from utils.ingestion_utils import table_function_mapping
from utils.bronze_utils import bronze_selected_columns
from utils.silver_utils import silver_selected_columns


def main():
    #table_create_and_ingest() => repeated for all data sources for each in table_sources_mapping
    
    RAW_DB_DEV = "dot_dev.db"
    RAW_DB_PROD = "dot_raw_prod.db"

    BRONZE_DB_DEV = "dot_dev_bronze.db"
    BRONZE_DB_PROD = "dot_bronze_prod.db"

    SILVER_DB_DEV = "dot_dev_silver.db"
    SILVER_DB_PROD = "dot_silver_prod.db"
    #ENVIRONMENT = RAW_DB_DEV
    
    
    for table in table_function_mapping:
        table_create_and_ingest(RAW_DB_DEV,table)
    
    # needs to be corrected, because all tables must be processed to bronze layer as well!
    for table_name in bronze_selected_columns:
        #cols = selected_columns.get(table)
        #if len(cols) == 0:
        #    pass
        #else:
            bronze_transformation(RAW_DB_DEV, table_name, BRONZE_DB_DEV)
    
    # process data from bronze to silver once previous iteration finished
    #bronze_to_silver_transformation
    for table_name in silver_selected_columns:
        bronze_transformation(RAW_DB_DEV, table_name, BRONZE_DB_DEV)

if __name__ == "__main__":
    main()
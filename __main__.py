from scripts.job import table_create_and_ingest, bronze_transformation, bronze_to_silver_transformation, silver_to_gold_transformation
from utils.ingestion_utils import table_function_mapping
from utils.bronze_utils import selected_columns as bronze_selected_columns
from utils.silver_utils import selected_columns as silver_selected_columns
from utils.gold_utils import selected_columns as gold_selected_columns, table_rename

from utils.utils import logger


def main():
    main = logger("process is hereby started", "main.py")
    main.new_or_existing_run()
    #table_create_and_ingest() => repeated for all data sources for each in table_sources_mapping
    DEV_RAW_DB = "dot_dev.db"
    DEV_BRONZE_DB = "dot_dev_bronze.db"
    DEV_SILVER_DB = "dot_dev_silver.db"
    DEV_GOLD_DB = "dot_dev_gold.db"

    PROD_RAW_DB = "dot_raw_prod.db"
    PROD_BRONZE_DB = "dot_bronze_prod.db"
    PROD_SILVER_DB = "dot_silver_prod.db"
       
    # API ingestion to Raw
    for table_name in table_function_mapping:
        table_create_and_ingest(DEV_RAW_DB,table_name)
    
    # Raw to Bronze => only selected columns
    for table_name in bronze_selected_columns:
        cols = bronze_selected_columns.get(table_name)
        if len(cols) == 0:
            pass
        else:
            bronze_transformation(DEV_RAW_DB, table_name, DEV_BRONZE_DB)

    # Bronze to Sivler => only selected columns
    for table_name in silver_selected_columns:
        bronze_to_silver_transformation(DEV_BRONZE_DB, table_name, DEV_SILVER_DB)


    # Silver to gold
    for silver_table_name, gold_table_name in table_rename.items():
            silver_to_gold_transformation(DEV_SILVER_DB, silver_table_name, DEV_GOLD_DB, gold_table_name)
    
    main.rename_log_file()

if __name__ == "__main__":
    main()
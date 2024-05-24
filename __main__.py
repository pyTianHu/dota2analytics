from scripts.job import table_create_and_ingest, bronze_transformation, bronze_to_silver_transformation, silver_to_gold_transformation
from scripts.facts_ingestion import Facts_Ingestion
from utils.ingestion_utils import table_function_mapping, fact_tables_mapping
from utils.bronze_utils import selected_columns as bronze_selected_columns
from utils.silver_utils import selected_columns as silver_selected_columns
from utils.gold_utils import selected_columns as gold_selected_columns, table_rename

from utils.utils import logger


def main():
    #table_create_and_ingest() => repeated for all data sources for each in table_sources_mapping
    env = "DEV"
    if env == "DEV":
        RAW_DB = "dot_dev.db"
        BRONZE_DB = "dot_dev_bronze.db"
        SILVER_DB = "dot_dev_silver.db"
        GOLD_DB = "dot_dev_gold.db"
    elif env == "PROD":
        RAW_DB = "dot_raw_prod.db"
        BRONZE_DB = "dot_bronze_prod.db"
        SILVER_DB = "dot_silver_prod.db"
       
    # API ingestion to Raw - dim tables
    for table_name in table_function_mapping:
        table_create_and_ingest(RAW_DB,table_name)
    
    # API ingestion to Raw - fact tables
    for table_name in fact_tables_mapping:
        fact_obj = Facts_Ingestion(RAW_DB,table_name)
        fact_obj.publicmatches_ingestion()

    # Raw to Bronze => only selected columns
    for table_name in bronze_selected_columns:
        cols = bronze_selected_columns.get(table_name)
        if len(cols) == 0:
            pass
        else:
            bronze_transformation(RAW_DB, table_name, BRONZE_DB)

    # Bronze to Sivler => only selected columns
    for table_name in silver_selected_columns:
        bronze_to_silver_transformation(BRONZE_DB, table_name, SILVER_DB)


    # Silver to gold
    for silver_table_name, gold_table_name in table_rename.items():
            silver_to_gold_transformation(SILVER_DB, silver_table_name, GOLD_DB, gold_table_name)
    

if __name__ == "__main__":
    main()
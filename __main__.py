from scripts.job import table_create_and_ingest, bronze_transformation
from utils.utils import table_function_mapping, selected_columns


def main():
    #table_create_and_ingest() => repeated for all data sources for each in table_sources_mapping
    environment = "dot_dev.db"
    
    for table in table_function_mapping:
        print(table)
        table_create_and_ingest(environment,table)
    
    for table in selected_columns:
        t = selected_columns.get(table)
        if len(t) == 0:
            pass
        else:
            bronze_transformation('dot_dev.db', t)


if __name__ == "__main__":
    main()
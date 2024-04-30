from scripts.job import table_create_and_ingest
from utils.utils import table_function_mapping


def main():
    #table_create_and_ingest() => repeated for all data sources for each in table_sources_mapping
    environment = "dot_dev.db"
    
    for table in table_function_mapping:
        print(table)
        table_create_and_ingest(environment,table)


if __name__ == "__main__":
    main()
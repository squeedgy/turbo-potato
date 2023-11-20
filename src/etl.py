import yaml
import logging
import json
from mysql_connector import MySQLConnector
from mongodb_connector import MongoDBConnector

def load_config(file_path='config.yaml'):
    with open(file_path, 'r') as config_file:
        config = yaml.safe_load(config_file)
    return config

def etl_process(mysql_config, mongodb_config, mysql_query, data_transform_func):
    mysql = MySQLConnector(**mysql_config)
    mysql.connect()

    mongo = MongoDBConnector(**mongodb_config)
    mongo.connect()

    try:
        mysql_result = mysql.execute_query(mysql_query)

        if mysql_result:
            transformed_data = data_transform_func(mysql_result)

            inserted_ids = mongo.insert_data(transformed_data)
            logging.info(f"Inserted data into MongoDB with IDs: {inserted_ids}")

    finally:
        mysql.disconnect()
        mongo.disconnect()

def simple_data_transform(mysql_data):
    return [{'new_key': row['old_key']} for row in mysql_data]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    config = load_config()
    mysql_config = config['mysql']
    mongodb_config = config['mongodb']

    mysql_query = "SELECT * FROM table;"
    data_transform_func = simple_data_transform

    etl_process(mysql_config, mongodb_config, mysql_query, data_transform_func)
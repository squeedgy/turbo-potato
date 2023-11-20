import mysql.connector
from mysql.connector import Error
import logging

class MySQLConnector:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                logging.info(f"Connected to MySQL database '{self.database}'")
        except Error as e:
            logging.error(f"Error connecting to MySQL: {e}")

    def disconnect(self):
        if self.connection.is_connected():
            self.connection.close()
            logging.info("Disconnected from MySQL")

    def execute_query(self, query, params=None):
        try:
            with self.connection.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                return cursor.fetchall()
        except Error as e:
            logging.error(f"Error executing query: {e}")
            return None
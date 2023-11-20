This is a ETL script written in Python for migrating data from MySQL to MongoDB. 

The script connects to a MySQL database, retrieves data, performs a basic transformation, and inserts it into a MongoDB database.

Getting Started
Clone the repository:

git clone https://github.com/your-username/mysql-to-mongodb-etl.git

Create a virtual environment and install dependencies:
conda env create -f environment.yml

Configure your config.yaml file with your MySQL and MongoDB connection details.

Configuration
Modify the config.yaml file to include your MySQL and MongoDB connection details. You can customize the MySQL query and data transformation functions in etl.py based on your specific requirements.

Dependencies
pymongo
mysql-connector-python
pyyaml

Happy ETLing!
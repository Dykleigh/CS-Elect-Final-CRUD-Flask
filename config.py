import os 

class Config:
    MYSQL_USER = os.getenv('MYSQL_USER', 'root')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'root123')
    MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
    MYSQL_DB = os.getenv('MYSQL_DB', 'hanz_sales_db')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
    MYSQL_POOL_NAME = os.getenv('MYSQL_POOL_NAME', 'mypool')
    MYSQL_POOL_SIZE = int(os.getenv('MYSQL_POOL_SIZE', 5))
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'your_jwt_secret_key')
    API_USERNAME = os.getenv('API_USERNAME', 'admin')
    API_PASSWORD = os.getenv('API_PASSWORD', 'password')
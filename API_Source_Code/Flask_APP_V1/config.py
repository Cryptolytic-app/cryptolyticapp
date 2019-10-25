import os

""" Retrieving Credintials Privately """

# PostgreSQL Database Credentials
POSTGRES_ADDRESS = os.environ.get("POSTGRES_ADDRESS")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_USERNAME = os.environ.get("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_DBNAME = os.environ.get("POSTGRES_DBNAME")

# AWS Access Key Crediantials
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY") 
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")

# AWS S3 Bucket Name
BUCKET_NAME = os.environ.get("BUCKET_NAME")
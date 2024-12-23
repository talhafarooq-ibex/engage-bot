import string
import urllib

from decouple import config
from langchain_mongodb.chat_message_histories import MongoDBChatMessageHistory
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient

punc = string.punctuation
punc = punc.replace('_', '')

host = config("DATABASE_HOST")
username = config("DATABASE_USERNAME")
password = config("DATABASE_PASSWORD")
database = config("DATABASE_NAME")
slug_db = config("SLUG_DATABASE")

async def connect(database = database):
    client = AsyncIOMotorClient(
        host=host,
        username=username,
        password=password
    )
    return client[database]

async def database_names():
    if username and password:  
        username_encoded = urllib.parse.quote_plus(username)
        password_encoded = urllib.parse.quote_plus(password)
                
        connection_string = f"mongodb://{username_encoded}:{password_encoded}@{host}:27017"
    else:   
        connection_string = f"mongodb://{host}:27017"

    client = AsyncIOMotorClient(connection_string)
    
    db_names = await client.list_database_names()
    filtered_db_names = [temp for temp in db_names if slug_db in temp]

    return filtered_db_names

def connect_sync(database = database):
    client = MongoClient(
        host=host,
        username=username,
        password=password
    )
    return client[database]

def format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)

def get_limited_message_history(session_id, connection_string, database_name, collection_name):
    history = MongoDBChatMessageHistory(
        session_id=session_id,
        connection_string=connection_string,
        database_name=database_name,
        collection_name=collection_name,
    )

    return history
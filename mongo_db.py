import pymongo
from bson import ObjectId
import certifi
from pymongo.mongo_client import MongoClient

def get_database(config):
    CONNECTION_STRING = config.get("MONGO_URL")

    # Create a new client and connect to the server
    client = MongoClient(CONNECTION_STRING, tlsCAFile=certifi.where())

    # Send a ping to confirm a successful connection
    client.admin.command('ping')
    return client['prisma']



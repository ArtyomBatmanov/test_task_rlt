import bson
import pymongo

from config import MONGO_DB, COLLECTION


def upload_data():
    with pymongo.MongoClient(MONGO_DB) as client:
        db = client["salaries"]
        with open(COLLECTION, "rb") as file:
            raw_data = file.read()
            decoded_data = bson.decode_all(raw_data)

            salary_collection = db["salary"]
            salary_collection.insert_many(decoded_data)


if __name__ == "__main__":
    upload_data()

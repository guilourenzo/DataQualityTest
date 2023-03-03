import pymongo

class MongoCRUD:
    def __init__(self, database_name, collection_name):
        self.client = pymongo.MongoClient()
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]

    def create(self, data):
        try:
            self.collection.insert_one(data)
            print("Data inserted successfully.")
        except Exception as e:
            print(f"Error while inserting data: {e}")

    def read(self, query={}):
        try:
            data = self.collection.find(query)
            return list(data)
        except Exception as e:
            print(f"Error while fetching data: {e}")

    def update(self, query, data):
        try:
            self.collection.update_one(query, {"$set": data})
            print("Data updated successfully.")
        except Exception as e:
            print(f"Error while updating data: {e}")

    def delete(self, query):
        try:
            self.collection.delete_one(query)
            print("Data deleted successfully.")
        except Exception as e:
            print(f"Error while deleting data: {e}")

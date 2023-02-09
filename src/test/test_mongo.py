import pymongo

class MongoCRUD:
    def __init__(self, database_name, collection_name):
        self.client = pymongo.MongoClient('mongodb://guilo:903041@localhost:27017/')
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
            self.collection.update_one(query, data)
            print("Data updated successfully.")
        except Exception as e:
            print(f"Error while updating data: {e}")

    def delete(self, query):
        try:
            self.collection.delete_one(query)
            print("Data deleted successfully.")
        except Exception as e:
            print(f"Error while deleting data: {e}")


def test_mongo_crud():
    # Create an instance of the MongoCRUD class
    crud = MongoCRUD("test_db", "test_collection")

    # Test create method
    data = {"name": "John", "age": 30}
    crud.create(data)
    inserted_data = crud.read({"name": "John"})
    assert inserted_data[0]["name"] == "John"

    # Test read method
    data = crud.read()
    assert len(data) > 0

    # Test update method
    data = {"name": "John"}
    new_data = {"$set": {"age": 35}}
    crud.update(data, new_data)
    updated_data = crud.read({"name": "John"})
    assert updated_data[0]["age"] == 35

    # Test delete method
    data = {"name": "John"}
    crud.delete(data)
    deleted_data = crud.read({"name": "John"})
    assert len(deleted_data) == 0

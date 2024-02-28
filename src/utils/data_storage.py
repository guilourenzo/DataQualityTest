from pymongo import MongoClient

class mongo_connect(object):
    def __init__(self, URI, DATABASE, COLLECTION):
        self.client = MongoClient(URI)
        self.DATABASE = self.client.get_database(DATABASE)
        self.COLLECTION = self.DATABASE.get_collection(COLLECTION)


    @staticmethod
    def insert_data(self, data) -> any:
        self.COLLECTION.insert_many(data)
    
    @staticmethod
    def find_data(self, QUERY):
        return self.COLLECTION.find(QUERY)
    
    @staticmethod
    def find_first_data(self, QUERY):
        return self.COLLECTION.find_one(QUERY)
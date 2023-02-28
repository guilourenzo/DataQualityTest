import unittest
from pymongo import MongoClient
from app import register_expectations

class TestRegisterExpectations(unittest.TestCase):

    def setUp(self):
        self.mongo_client = MongoClient("mongodb://localhost:27017/")
        self.database_name = "test_database"
        self.collection_name = "test_collection"

    def test_register_expectations(self):
        data_source = {
            "source_type": "csv",
            "file_path": "test_data.csv",
            "header_row": True
        }
        data_columns = ["col1", "col2"]
        expectations = [
            {
                "column": "col1",
                "expectations": [
                    {
                        "expectation": "expect_column_values_to_not_be_null",
                        "kwargs": {}
                    }
                ]
            },
            {
                "column": "col2",
                "expectations": [
                    {
                        "expectation": "expect_column_values_to_not_be_null",
                        "kwargs": {}
                    },
                    {
                        "expectation": "expect_column_values_to_be_unique",
                        "kwargs": {}
                    }
                ]
            }
        ]

        register_expectations(self.mongo_client, self.database_name, self.collection_name, data_source, data_columns, expectations)

        db = self.mongo_client[self.database_name]
        db_coll = db[self.collection_name]
        result = db_coll.find_one()

        expected_result = {
            "data_source": {
                "source_type": "csv",
                "file_path": "test_data.csv",
                "header_row": True
            },
            "data_columns": ["col1", "col2"],
            "expectations": [
                {
                    "column": "col1",
                    "expectations": [
                        {
                            "expectation": "expect_column_values_to_not_be_null",
                            "kwargs": {}
                        }
                    ]
                },
                {
                    "column": "col2",
                    "expectations": [
                        {
                            "expectation": "expect_column_values_to_not_be_null",
                            "kwargs": {}
                        },
                        {
                            "expectation": "expect_column_values_to_be_unique",
                            "kwargs": {}
                        }
                    ]
                }
            ]
        }

        self.assertEqual(result, expected_result)

    def tearDown(self):
        self.mongo_client.drop_database(self.database_name)

if __name__ == '__main__':
    unittest.main()

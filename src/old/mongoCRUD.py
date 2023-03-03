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

"""
Unit tests for the inventory API endpoints.

This module uses FastAPI's TestClient to send requests to the API
and assert that the responses are as expected. Each test function
focuses on a specific endpoint or a particular aspect of an endpoint's behavior.
The in-memory database (`db_items` in `services.inventory_service`) is reset
before each test to ensure test isolation.
"""
import uuid
from fastapi.testclient import TestClient
from main import app
# Item model is not strictly needed for these tests as we work with dicts from JSON,
# but could be useful for more complex scenarios or if constructing Item objects in tests.
# from models.item import Item
from services.inventory_service import reset_db

client = TestClient(app)

def create_item_via_api(name: str = "Test Item", description: str = "Test Description", price: float = 10.0, quantity: int = 5) -> dict:
    """
    Helper function to create an item via the API and return its JSON response.
    Asserts that the creation was successful (status code 201).
    """
    response = client.post("/inventory/items", json={
        "name": name,
        "description": description,
        "price": price,
        "quantity": quantity
    })
    assert response.status_code == 201, f"Failed to create item. Status: {response.status_code}, Body: {response.text}"
    return response.json()

def test_create_item():
    """Test creating a new item successfully via POST /inventory/items."""
    reset_db()
    item_data = {"name": "Laptop", "description": "High-end gaming laptop", "price": 1500.0, "quantity": 10}
    response = client.post("/inventory/items", json=item_data)
    assert response.status_code == 201
    created_item = response.json()
    # Check that all provided fields are present and correct in the response
    assert created_item["name"] == item_data["name"]
    assert created_item["description"] == item_data["description"]
    assert created_item["price"] == item_data["price"]
    assert created_item["quantity"] == item_data["quantity"]
    # Check that a server-generated ID is present
    assert "id" in created_item
    assert uuid.UUID(created_item["id"]) # Ensure 'id' is a valid UUID

def test_read_items():
    """Test reading all items successfully via GET /inventory/items."""
    reset_db()
    # Create a couple of items to ensure the list is not empty
    item1_data = create_item_via_api(name="Keyboard", price=75.0, quantity=20)
    item2_data = create_item_via_api(name="Mouse", price=25.0, quantity=30)

    response = client.get("/inventory/items")
    assert response.status_code == 200
    items = response.json()
    assert isinstance(items, list)
    assert len(items) == 2
    # Verify that the IDs of the created items are present in the response list
    response_item_ids = [item['id'] for item in items]
    assert item1_data['id'] in response_item_ids
    assert item2_data['id'] in response_item_ids

def test_read_item():
    """Test reading a single item by its ID successfully via GET /inventory/items/{item_id}."""
    reset_db()
    created_item_data = create_item_via_api(name="Specific Item For Read Test")
    item_id = created_item_data["id"]

    response = client.get(f"/inventory/items/{item_id}")
    assert response.status_code == 200
    item = response.json()
    # Verify the retrieved item matches the created item
    assert item["id"] == item_id
    assert item["name"] == created_item_data["name"]

def test_read_item_not_found():
    """Test that GET /inventory/items/{item_id} returns 404 for a non-existent item ID."""
    reset_db()
    non_existent_uuid = str(uuid.uuid4()) # Generate a random, valid UUID
    response = client.get(f"/inventory/items/{non_existent_uuid}")
    # Expect a 404 Not Found error
    assert response.status_code == 404

def test_update_item():
    """Test updating an existing item successfully via PUT /inventory/items/{item_id}."""
    reset_db()
    created_item_data = create_item_via_api(name="Old Name", price=10.0, description="Original Description")
    item_id = created_item_data["id"]

    update_data = {"name": "New Name", "price": 12.50} # Partial update
    response = client.put(f"/inventory/items/{item_id}", json=update_data)
    assert response.status_code == 200
    updated_item_response = response.json()
    # Check that the response reflects the updates
    assert updated_item_response["name"] == update_data["name"]
    assert updated_item_response["price"] == update_data["price"]
    # Description should remain unchanged as it wasn't in update_data
    assert updated_item_response["description"] == created_item_data["description"]

    # Verify that the update persisted by fetching the item again
    response_get = client.get(f"/inventory/items/{item_id}")
    assert response_get.status_code == 200
    item_from_db = response_get.json()
    assert item_from_db["name"] == update_data["name"]
    assert item_from_db["price"] == update_data["price"]

def test_update_item_not_found():
    """Test that PUT /inventory/items/{item_id} returns 404 for a non-existent item ID."""
    reset_db()
    non_existent_uuid = str(uuid.uuid4())
    update_data = {"name": "This Should Not Matter"}
    response = client.put(f"/inventory/items/{non_existent_uuid}", json=update_data)
    # Expect a 404 Not Found error
    assert response.status_code == 404

def test_delete_item():
    """Test deleting an existing item successfully via DELETE /inventory/items/{item_id}."""
    reset_db()
    created_item_data = create_item_via_api()
    item_id = created_item_data["id"]

    # Delete the item
    delete_response = client.delete(f"/inventory/items/{item_id}")
    assert delete_response.status_code == 204 # 204 No Content indicates success

    # Verify the item is actually gone by trying to fetch it
    get_response_after_delete = client.get(f"/inventory/items/{item_id}")
    assert get_response_after_delete.status_code == 404 # Should now be Not Found

def test_delete_item_not_found():
    """Test that DELETE /inventory/items/{item_id} returns 404 for a non-existent item ID."""
    reset_db()
    non_existent_uuid = str(uuid.uuid4())
    response = client.delete(f"/inventory/items/{non_existent_uuid}")
    # Expect a 404 Not Found error
    assert response.status_code == 404

def test_search_items_by_name():
    """Test searching items by name via GET /inventory/items/search?name=..."""
    reset_db()
    # Create a set of items for searching
    create_item_via_api(name="Apple iPhone 13", description="Latest iPhone model")
    create_item_via_api(name="Samsung Galaxy S21", description="Latest Samsung phone")
    create_item_via_api(name="Apple MacBook Pro", description="Powerful laptop")

    # Search for items containing "Apple" in the name
    response = client.get("/inventory/items/search?name=Apple")
    assert response.status_code == 200
    items = response.json()
    assert len(items) == 2 # Expecting two "Apple" products
    for item in items:
        assert "apple" in item["name"].lower() # Case-insensitive check

def test_search_items_by_description():
    """Test searching items by description via GET /inventory/items/search?description=..."""
    reset_db()
    create_item_via_api(name="Monitor", description="Large 4K Monitor for productivity")
    create_item_via_api(name="Desk Lamp", description="Bright LED Desk Lamp")
    create_item_via_api(name="Gaming PC", description="High-end PC for gaming")

    # Search for items containing "Lamp" in the description
    response = client.get("/inventory/items/search?description=Lamp")
    assert response.status_code == 200
    items = response.json()
    assert len(items) == 1
    assert "lamp" in items[0]["description"].lower() # Case-insensitive check

def test_search_items_by_name_and_description_revised():
    """
    Test searching items by both name and description via 
    GET /inventory/items/search?name=...&description=...
    This revised test ensures clearer setup and assertions for combined search criteria.
    """
    reset_db()
    # Item that matches both name and description query
    create_item_via_api(name="Cool Gadget X1", description="Very cool and useful gadget")
    # Item that matches name query only
    create_item_via_api(name="Cool Gadget Y2", description="Another type of device")
    # Item that matches description query only
    create_item_via_api(name="Random Item Z3", description="A cool accessory for your setup")
    # Item that matches neither query
    create_item_via_api(name="Generic Product A4", description="Standard item")

    # Search for items matching "Cool Gadget" in name AND "cool" in description
    response = client.get("/inventory/items/search?name=Cool Gadget&description=cool")
    assert response.status_code == 200
    items = response.json()
    assert len(items) == 1, "Should find only 'Cool Gadget X1'"
    assert items[0]["name"] == "Cool Gadget X1"
    assert "cool" in items[0]["description"].lower()
    assert "cool gadget" in items[0]["name"].lower()

    # Test with only name, expecting two results ("Cool Gadget X1", "Cool Gadget Y2")
    response_name_only = client.get("/inventory/items/search?name=Cool Gadget")
    assert response_name_only.status_code == 200
    items_name_only = response_name_only.json()
    assert len(items_name_only) == 2

    # Test with only description, expecting two results ("Cool Gadget X1", "Random Item Z3")
    response_desc_only = client.get("/inventory/items/search?description=cool")
    assert response_desc_only.status_code == 200
    items_desc_only = response_desc_only.json()
    assert len(items_desc_only) == 2

    # Test with name and a non-matching description, expecting zero results
    response_name_non_match_desc = client.get("/inventory/items/search?name=Cool Gadget&description=nonexistent")
    assert response_name_non_match_desc.status_code == 200
    items_name_non_match_desc = response_name_non_match_desc.json()
    assert len(items_name_non_match_desc) == 0

    # Test with description and a non-matching name, expecting zero results
    response_desc_non_match_name = client.get("/inventory/items/search?description=cool&name=nonexistent")
    assert response_desc_non_match_name.status_code == 200
    items_desc_non_match_name = response_desc_non_match_name.json()
    assert len(items_desc_non_match_name) == 0


def test_search_items_no_results():
    """Test that GET /inventory/items/search returns an empty list when no items match the criteria."""
    reset_db()
    # Create an item that will not match the search term
    create_item_via_api(name="Existing Item To Be Ignored")
    
    response = client.get("/inventory/items/search?name=nonexistentsearchterm")
    assert response.status_code == 200
    items = response.json()
    assert isinstance(items, list)
    assert len(items) == 0 # Expect an empty list

def test_search_items_no_query_params():
    """
    Test that GET /inventory/items/search returns all items when no query parameters are provided.
    This assumes the service's search function returns all items if no criteria are given.
    """
    reset_db()
    item1_data = create_item_via_api(name="Item One For No Query Search")
    item2_data = create_item_via_api(name="Item Two For No Query Search")
    
    response = client.get("/inventory/items/search") # No query parameters
    assert response.status_code == 200
    items = response.json()
    assert len(items) == 2 # Should return all items created in this test
    
    # Verify that the IDs of the created items are present in the response list
    response_item_ids = [item['id'] for item in items]
    assert item1_data['id'] in response_item_ids
    assert item2_data['id'] in response_item_ids


# Note: The original `test_search_items_by_name_and_description` was removed in favor of
# `test_search_items_by_name_and_description_revised` as the revised version is more comprehensive
# and has clearer setup for its assertions.

# General structure:
# - `reset_db()` is called at the start of each test to ensure a clean state.
# - `create_item_via_api` helper simplifies test setup for item creation.
# - Assertions check status codes, response body structure, and specific field values.
# - Tests for "not found" scenarios ensure 404 errors are correctly returned.
# - Search tests cover various combinations of parameters and expected outcomes.

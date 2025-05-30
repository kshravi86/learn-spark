from typing import List, Optional
import uuid
import logging
from models.item import Item, ItemUpdate

logger = logging.getLogger(__name__)

# In-memory list to store inventory items, acting as a simple database.
db_items: List[Item] = []

def create_item(item: Item) -> Item:
    logger.info(f"Attempting to create item: {item.model_dump()}")
    db_items.append(item)
    logger.info(f"Successfully created item with ID: {item.id}")
    return item

def get_items() -> List[Item]:
    logger.info("Retrieving all items.")
    return db_items

def get_item_by_id(item_id: uuid.UUID) -> Optional[Item]:
    logger.info(f"Searching for item with ID: {item_id}")
    for item_in_db in db_items:
        if item_in_db.id == item_id:
            logger.info(f"Item found with ID: {item_id}")
            return item_in_db
    logger.info(f"Item not found with ID: {item_id}")
    return None

def update_item_by_id(item_id: uuid.UUID, item_update: ItemUpdate) -> Optional[Item]:
    logger.info(f"Attempting to update item with ID: {item_id}. Update data: {item_update.model_dump(exclude_unset=True)}")
    for item_in_db in db_items:
        if item_in_db.id == item_id:
            # Apply partial updates: only fields present in item_update (and not None) are modified.
            # .model_dump(exclude_unset=True) ensures that only fields explicitly set in the Pydantic model
            # by the client are included in the update_data dictionary.
            update_data = item_update.model_dump(exclude_unset=True)
            for key, value in update_data.items():
                setattr(item_in_db, key, value)
            logger.info(f"Successfully updated item with ID: {item_id}")
            return item_in_db
    logger.info(f"Item not found with ID: {item_id}. Update failed.")
    return None

def delete_item_by_id(item_id: uuid.UUID) -> bool:
    global db_items
    logger.info(f"Attempting to delete item with ID: {item_id}")
    initial_len = len(db_items)
    db_items = [item for item in db_items if item.id != item_id]
    if len(db_items) < initial_len:
        logger.info(f"Successfully deleted item with ID: {item_id}")
        return True
    logger.info(f"Item not found with ID: {item_id}. Deletion failed.")
    return False

def search_items(name: Optional[str] = None, description: Optional[str] = None) -> List[Item]:
    logger.info(f"Searching items with name: '{name}' and description: '{description}'")
    results = db_items # Start with all items and filter down.

    # Case-insensitive search for name: checks if the lowercase 'name' query is a substring of the item's lowercase name.
    if name:
        results = [item for item in results if name.lower() in item.name.lower()]
    
    # Case-insensitive search for description: checks if the lowercase 'description' query is a substring of the item's lowercase description.
    # Also ensures item.description is not None before attempting to search within it.
    if description:
        results = [item for item in results if item.description and description.lower() in item.description.lower()]
    
    logger.info(f"Found {len(results)} items matching search criteria.")
    return results

def reset_db():
    global db_items
    logger.info("Resetting in-memory database (db_items).")
    db_items = []

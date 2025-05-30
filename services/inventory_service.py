from typing import List, Optional
import uuid
from models.item import Item, ItemUpdate

db_items: List[Item] = []

def create_item(item: Item) -> Item:
    db_items.append(item)
    return item

def get_items() -> List[Item]:
    return db_items

def get_item_by_id(item_id: uuid.UUID) -> Optional[Item]:
    for item in db_items:
        if item.id == item_id:
            return item
    return None

def update_item_by_id(item_id: uuid.UUID, item_update: ItemUpdate) -> Optional[Item]:
    for item in db_items:
        if item.id == item_id:
            update_data = item_update.model_dump(exclude_unset=True)
            for key, value in update_data.items():
                setattr(item, key, value)
            return item
    return None

def delete_item_by_id(item_id: uuid.UUID) -> bool:
    global db_items
    initial_len = len(db_items)
    db_items = [item for item in db_items if item.id != item_id]
    return len(db_items) < initial_len

def search_items(name: Optional[str] = None, description: Optional[str] = None) -> List[Item]:
    results = db_items
    if name:
        results = [item for item in results if name.lower() in item.name.lower()]
    if description:
        results = [item for item in results if item.description and description.lower() in item.description.lower()]
    return results

def reset_db():
    global db_items
    db_items = []

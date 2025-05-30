import uuid
from typing import List
from fastapi import APIRouter, HTTPException, status, Response
from models.item import Item, ItemUpdate
from services.inventory_service import (
from typing import Optional
from services.inventory_service import (
    create_item as create_item_service,
    get_items as get_items_service,
    get_item_by_id as get_item_by_id_service,
    update_item_by_id as update_item_by_id_service,
    delete_item_by_id as delete_item_by_id_service,
    search_items as search_items_service
)

router = APIRouter()

@router.post("/items", response_model=Item, status_code=status.HTTP_201_CREATED)
async def create_new_item(item: Item):
    return create_item_service(item)

@router.get("/items", response_model=List[Item])
async def read_items():
    return get_items_service()

@router.get("/items/search", response_model=List[Item])
async def search_inventory_items(name: Optional[str] = None, description: Optional[str] = None):
    return search_items_service(name=name, description=description)

@router.get("/items/{item_id}", response_model=Item)
async def read_item(item_id: uuid.UUID):
    item = get_item_by_id_service(item_id)
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
    return item

@router.put("/items/{item_id}", response_model=Item)
async def update_existing_item(item_id: uuid.UUID, item_update: ItemUpdate):
    updated_item = update_item_by_id_service(item_id, item_update)
    if updated_item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
    return updated_item

@router.delete("/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_existing_item(item_id: uuid.UUID):
    deleted = delete_item_by_id_service(item_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)

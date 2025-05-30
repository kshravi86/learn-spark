import uuid
import logging
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

logger = logging.getLogger(__name__)
router = APIRouter()

# Note on route order: FastAPI matches routes in the order they are defined.
# If a path can match multiple routes, the first one defined will be used.
# For example, if `/items/search` was defined after `/items/{item_id}`,
# a request to `/items/search` might be incorrectly captured by `/items/{item_id}`,
# treating "search" as an item_id. Using distinct paths like `/items/search` avoids this ambiguity.

@router.post("/items", response_model=Item, status_code=status.HTTP_201_CREATED)
async def create_new_item(item: Item):
    logger.info(f"Received POST request to create item with data: {item.model_dump()}")
    # status_code=201 CREATED is used to indicate that a new resource has been successfully created.
    new_item = create_item_service(item)
    logger.info(f"Item created successfully with ID: {new_item.id}. Responding with status 201.")
    return new_item

@router.get("/items", response_model=List[Item])
async def read_items():
    logger.info("Received GET request for all items.")
    items = get_items_service()
    logger.info(f"Responding with {len(items)} items. Status 200.")
    return items

@router.get("/items/search", response_model=List[Item])
async def search_inventory_items(name: Optional[str] = None, description: Optional[str] = None):
    """
    Search for inventory items based on name and/or description.
    Query parameters are optional.
    """
    logger.info(f"Received GET request to search items with params: name='{name}', description='{description}'.")
    items = search_items_service(name=name, description=description)
    logger.info(f"Search found {len(items)} items. Responding with status 200.")
    return items

@router.get("/items/{item_id}", response_model=Item)
async def read_item(item_id: uuid.UUID):
    logger.info(f"Received GET request for item ID: {item_id}.")
    item = get_item_by_id_service(item_id)
    if item is None:
        # HTTPException is used to return an HTTP error response to the client.
        # 404 NOT FOUND is the standard response for a non-existent resource.
        logger.warning(f"Item with ID: {item_id} not found. Raising 404.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
    logger.info(f"Item with ID: {item_id} found. Responding with status 200.")
    return item

@router.put("/items/{item_id}", response_model=Item)
async def update_existing_item(item_id: uuid.UUID, item_update: ItemUpdate):
    logger.info(f"Received PUT request to update item ID: {item_id} with data: {item_update.model_dump(exclude_unset=True)}.")
    updated_item = update_item_by_id_service(item_id, item_update)
    if updated_item is None:
        # Raise 404 if the item to update is not found.
        logger.warning(f"Item with ID: {item_id} not found for update. Raising 404.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
    logger.info(f"Item with ID: {item_id} updated successfully. Responding with status 200.")
    return updated_item

@router.delete("/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_existing_item(item_id: uuid.UUID):
    logger.info(f"Received DELETE request for item ID: {item_id}.")
    # status_code=204 NO CONTENT indicates successful deletion with no response body.
    deleted = delete_item_by_id_service(item_id)
    if not deleted:
        # Raise 404 if the item to delete is not found.
        logger.warning(f"Item with ID: {item_id} not found for deletion. Raising 404.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
    # For 204 responses, FastAPI expects no body, so we return a Response with the status code.
    logger.info(f"Item with ID: {item_id} deleted successfully. Responding with status 204.")
    return Response(status_code=status.HTTP_204_NO_CONTENT)

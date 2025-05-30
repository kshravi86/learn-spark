# FastAPI Inventory Management API

This project is a simple RESTful API for managing an inventory of items, built with FastAPI. It allows users to perform Create, Read, Update, and Delete (CRUD) operations on inventory items, as well as search for items based on name and description.

## API Endpoints

All endpoints are prefixed with `/inventory`.

### Item Model

The `Item` has the following structure:
```json
{
  "id": "uuid",         // (auto-generated)
  "name": "string",
  "description": "string (optional)",
  "price": "float",
  "quantity": "integer"
}
```

### Create a new item
- **POST** `/items`
- **Description:** Adds a new item to the inventory.
- **Request Body:** `Item` (excluding `id`)
  ```json
  {
    "name": "Laptop",
    "description": "High-performance laptop",
    "price": 1200.50,
    "quantity": 10
  }
  ```
- **Successful Response:** `201 CREATED`
  - Body: The created `Item` object (including its new `id`).
- **Error Response:**
  - `422 Unprocessable Entity`: If request body is invalid.

### Get all items
- **GET** `/items`
- **Description:** Retrieves a list of all inventory items.
- **Successful Response:** `200 OK`
  - Body: A list of `Item` objects.
  ```json
  [
    {
      "id": "...", "name": "Laptop", ...
    },
    {
      "id": "...", "name": "Mouse", ...
    }
  ]
  ```

### Get a specific item
- **GET** `/items/{item_id}`
- **Description:** Retrieves a single item by its unique ID.
- **Path Parameter:** `item_id` (UUID)
- **Successful Response:** `200 OK`
  - Body: The requested `Item` object.
- **Error Response:**
  - `404 Not Found`: If no item with the given ID exists.

### Update an existing item
- **PUT** `/items/{item_id}`
- **Description:** Updates an existing item's details.
- **Path Parameter:** `item_id` (UUID)
- **Request Body:** `ItemUpdate` (all fields optional, similar to `Item` but `id` is not updatable)
  ```json
  {
    "name": "Updated Laptop Name", // Only fields to update
    "price": 1150.00
  }
  ```
- **Successful Response:** `200 OK`
  - Body: The updated `Item` object.
- **Error Response:**
  - `404 Not Found`: If no item with the given ID exists.
  - `422 Unprocessable Entity`: If request body is invalid.

### Delete an item
- **DELETE** `/items/{item_id}`
- **Description:** Removes an item from the inventory.
- **Path Parameter:** `item_id` (UUID)
- **Successful Response:** `204 No Content`
- **Error Response:**
  - `404 Not Found`: If no item with the given ID exists.

### Search for items
- **GET** `/items/search`
- **Description:** Searches for items based on query parameters.
- **Query Parameters:**
  - `name: Optional[str]` - Filter by item name (case-insensitive, partial match).
  - `description: Optional[str]` - Filter by item description (case-insensitive, partial match).
- **Example:** `/inventory/items/search?name=lap&description=perform`
- **Successful Response:** `200 OK`
  - Body: A list of `Item` objects matching the search criteria.

## Setup and Running the Project

### Prerequisites
- Python 3.8+
- pip (Python package installer)
- Git (for cloning the repository, optional if code is downloaded)

### Installation & Setup

1.  **Clone the repository (optional):**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Create and activate a virtual environment (recommended):**
    ```bash
    python -m venv venv
    # On Windows
    # venv\Scripts\activate
    # On macOS/Linux
    source venv/bin/activate
    ```

3.  **Install dependencies:**
    (Ensure you have a `requirements.txt` file in the project root)
    ```bash
    pip install -r requirements.txt
    ```

### Running the Application

Once dependencies are installed, you can run the FastAPI application using Uvicorn:

```bash
uvicorn main:app --reload
```

- `main:app` refers to the `app` instance in the `main.py` file.
- `--reload` enables auto-reloading when code changes are detected (useful for development).

The API will typically be available at `http://127.0.0.1:8000`. You can access the interactive API documentation (Swagger UI) at `http://127.0.0.1:8000/docs` and alternative documentation (ReDoc) at `http://127.0.0.1:8000/redoc`.

## Running Tests

This project uses `pytest` for unit testing. To run the tests:

1.  Ensure you have activated your virtual environment and installed dependencies (including `pytest`, which should be in `requirements.txt`).
2.  Navigate to the project root directory.
3.  Run the following command:

    ```bash
    pytest
    ```

This will discover and execute all tests in the `tests/` directory.

## Project Structure

```
.
├── main.py         # Main FastAPI application file
├── models/         # Pydantic models for data representation
│   └── item.py
├── routes/         # API route definitions
│   └── inventory.py
├── services/       # Business logic and data handling
│   └── inventory_service.py
├── tests/          # Unit tests
│   └── test_inventory.py
├── README.md       # This file
└── requirements.txt # Project dependencies
```

- **`main.py`**: Initializes the FastAPI application and includes routers.
- **`models/`**: Contains Pydantic models that define the structure of data (e.g., `Item`, `ItemUpdate`).
- **`routes/`**: Defines the API endpoints (paths, methods) and connects them to service functions.
- **`services/`**: Implements the core business logic for handling inventory operations and interacts with the data store (currently in-memory).
- **`tests/`**: Contains unit tests for the API endpoints and services.
- **`requirements.txt`**: Lists all Python package dependencies for the project.

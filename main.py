from fastapi import FastAPI
from routes import inventory

app = FastAPI()

app.include_router(inventory.router, prefix="/inventory", tags=["inventory"])

@app.get("/")
async def root():
    return {"message": "Hello World"}

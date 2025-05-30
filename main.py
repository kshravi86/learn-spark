import logging
from fastapi import FastAPI
from routes import inventory

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.include_router(inventory.router, prefix="/inventory", tags=["inventory"])

@app.on_event("startup")
async def startup_event():
    logger.info("Application startup complete.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown complete.")

@app.get("/")
async def root():
    logger.info("Root endpoint '/' was called.")
    return {"message": "Hello World"}

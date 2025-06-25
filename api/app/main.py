"""FastAPI application entry point for the Geodata API"""

from fastapi import FastAPI  # type: ignore
from api.app.routers import hexagons

app = FastAPI()

# Include routers
app.include_router(hexagons.router, prefix="/api/v1", tags=["Hexagons"])


@app.get("/")
def root():
    """
    Root endpoint for the Geodata API.
    Returns:
        dict: A welcome message.
    """
    return {"message": "Welcome to the Geodata API"}

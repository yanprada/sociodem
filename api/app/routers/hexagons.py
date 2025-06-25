"""Hexagons API Router"""

from typing import List
from fastapi import APIRouter, Depends  # type: ignore
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from api.app.database import get_db
from api.app.models import Hexagon
from api.app.schemas import HexagonSchema


router = APIRouter()


@router.get("/hexagons/", response_model=List[HexagonSchema])
async def get_hexagons(municipality_id: int, db: AsyncSession = Depends(get_db)):
    """
    Retrieve hexagons for a given municipality.
    Args:
        municipality_id (int): The ID of the municipality to filter hexagons.
        db (AsyncSession): The database session dependency.
    Returns:
        List[HexagonSchema]: A list of hexagons associated with the municipality.
    """
    query = select(Hexagon).filter(Hexagon.municipality_id == municipality_id)
    result = await db.execute(query)
    hexagons = result.scalars().all()
    return hexagons

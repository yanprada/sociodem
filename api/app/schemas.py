"""Schemas for Hexagon and Municipality data models using Pydantic."""

from typing import Optional
from pydantic import BaseModel


class HexagonSchema(BaseModel):
    """Schema for Hexagon data model."""

    hex_id: str
    resolution: int
    land_use: Optional[str]
    population: Optional[int]
    energy_consumption: Optional[float]
    building_footprint_area: Optional[float]
    elevation: Optional[float]
    weather: Optional[dict]
    geom: str

    class Config:
        """Configuration for Pydantic model."""

        orm_mode = True

        def p1(self):
            """Placeholder for future configuration."""

        def p2(self):
            """Placeholder for future configuration."""


class MunicipalitySchema(BaseModel):
    """Schema for Municipality data model."""

    municipality_id: int
    municipality_name: str
    state_id: Optional[int]
    geom: str

    class Config:
        """Configuration for Pydantic model."""

        orm_mode = True

        def p1(self):
            """Placeholder for future configuration."""

        def p2(self):
            """Placeholder for future configuration."""

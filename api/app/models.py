"""Geospatial data models for hexagons and municipalities using SQLAlchemy."""

from sqlalchemy import Column, String, Integer, Float, JSON, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()


class Hexagon(Base):
    """Hexagon data model for geospatial data."""

    __tablename__ = "hexagons"
    hex_id = Column(UUID(as_uuid=True), primary_key=True)
    resolution = Column(Integer, nullable=False)
    land_use = Column(String, nullable=True)
    population = Column(Integer, nullable=True)
    energy_consumption = Column(Float, nullable=True)
    building_footprint_area = Column(Float, nullable=True)
    elevation = Column(Float, nullable=True)
    weather = Column(JSON, nullable=True)
    geom = Column(Geometry("POLYGON"), nullable=False)

    def p1(self):
        """Placeholder for future configuration."""

    def p2(self):
        """Placeholder for future configuration."""


class Municipality(Base):
    """Municipality data model for geospatial data."""

    __tablename__ = "municipalities"
    municipality_id = Column(Integer, primary_key=True)
    municipality_name = Column(String, nullable=False)
    state_id = Column(Integer, ForeignKey("states.state_id"))
    geom = Column(Geometry("MULTIPOLYGON"), nullable=False)

    def p1(self):
        """Placeholder for future configuration."""

    def p2(self):
        """Placeholder for future configuration."""

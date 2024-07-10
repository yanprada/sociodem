"""
This module contains constants used in the sociodem project.

- CRS: The coordinate reference system used in the project.
- STATES: A list of Brazilian states.
"""

HEX_RESOLUTION = 10
CRS = "EPSG:4674"

MAPBIOMAS_CLASSES = {
    3: "Floresta",
    4: "Savana",
    5: "Mangue",
    6: "Floresta Alagavel",
    49: "Restinga Arborea",
    11: "Campo Alagado",
    12: "Formacao Campestre",
    50: "Restinga Herbacea",
    32: "Outras Formacoes Naturais nao Florestais",
    29: "Outras Formacoes Naturais nao Florestais",
    13: "Outras Formacoes Naturais nao Florestais",
    15: "Pastagem",
    39: "Soja",
    20: "Cana de Acucar",
    40: "Arroz",
    62: "Algodao",
    41: "Outras Lavouras temporarias",
    46: "Cafe",
    47: "Citrus",
    35: "Dende",
    48: "Outras Lavouras Perenes",
    9: "Silvicultura",
    21: "Mosaico de usos",
    24: "Area Urbana",
    30: "Mineracao",
    23: "Outras Areas nao Vegetadas",
    25: "Outras Areas nao Vegetadas",
    33: "Corpo d'agua",
    31: "Corpo d'agua",
    27: "Nao Observado",
}

STATES = {
    "AC": 12,
    "AL": 27,
    "AP": 16,
    "AM": 13,
    "BA": 29,
    "CE": 23,
    "DF": 53,
    "ES": 32,
    "GO": 52,
    "MA": 21,
    "MT": 51,
    "MS": 50,
    "MG": 31,
    "PA": 15,
    "PB": 25,
    "PR": 41,
    "PE": 26,
    "PI": 22,
    "RJ": 33,
    "RN": 24,
    "RS": 43,
    "RO": 11,
    "RR": 14,
    "SC": 42,
    "SP": 35,
    "SE": 28,
    "TO": 17,
}

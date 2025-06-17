"""
This module contains constants used in the sociodem project.

- CRS: The coordinate reference system used in the project.
- STATES: A list of Brazilian states.
"""

HEX_RESOLUTION = 10
CRS_IBGE = "EPSG:4674"
CRS_GLOBAL = "EPSG:4326"
OVERTURE_RELEASE_VERSION = "2025-05-21.0"
BBOX_BRAZIL = {
    "xmin": -73.9403379576,
    "ymin": -34.2093068356,
    "xmax": -32.3460996763,
    "ymax": 5.7482989621,
}


DOMPP_CLASSES = {
    1: "domicilio_particular",
    2: "domicilio_coletivo",
    3: "estabelecimento_agropecuario",
    4: "estabelecimento_ensino",
    5: "estabelecimento_saude",
    6: "outros_estabelecimentos",
    7: "edificio_em_construcao",
    8: "estabelecimento_religioso",
}

ANEEL_CLASSES = {
    "RE1": "residencial_urbano",
    "RE2": "residencial_urbano_baixa_renda",
    "RE3": "residencial_urbano_baixa_renda",
    "RE4": "residencial_urbano_baixa_renda",
    "RE5": "residencial_urbano_baixa_renda",
    "RE6": "residencial_urbano_baixa_renda",
    "RU3": "residencial_rural",
    "CO1": "comercial",
    "CO2": "comercial",
    "CO3": "comercial",
    "CO4": "comercial",
    "CO5": "comercial",
    "CO8": "comercial",
    "CO9": "comercial",
    "CO7": "rodovias",
    "CO6": "condominios",
    "IN": "industrial",
    "RU1": "agropecuaria",
    "RU1A": "agropecuaria",
    "RU1B": "agropecuaria",
    "RU2": "agropecuaria",
    "RU4": "agropecuaria",
    "RU5": "agropecuaria",
    "RU6": "agropecuaria",
    "RU7": "agropecuaria",
    "RU8": "agropecuaria",
    "PP1": "poder_publico",
    "PP2": "poder_publico",
    "PP3": "poder_publico",
    "IP": "servicos_publicos",
    "SP1": "servicos_publicos",
    "SP2": "servicos_publicos",
    "0": "outros",
    "CPR": "outros",
    "CPRVE": "outros",
    "CSPS": "outros",
}

MAPBIOMAS_CLASSES = {
    3: "floresta",
    4: "savana",
    5: "mangue",
    6: "floresta_alagavel",
    49: "restinga_arborea",
    11: "campo_alagado",
    12: "formacao_campestre",
    50: "restinga_herbacea",
    32: "outras_formacoes_naturais_nao_florestais",
    29: "outras_formacoes_naturais_nao_florestais",
    13: "outras_formacoes_naturais_nao_florestais",
    15: "pastagem",
    39: "soja",
    20: "cana_de_acucar",
    40: "arroz",
    62: "algodao",
    41: "outras_lavouras_temporarias",
    46: "cafe",
    47: "citrus",
    35: "dende",
    48: "outras_lavouras_perenes",
    9: "silvicultura",
    21: "mosaico_de_usos",
    24: "area_urbana",
    30: "mineracao",
    23: "outras_areas_nao_vegetadas",
    25: "outras_areas_nao_vegetadas",
    33: "corpo_dagua",
    31: "corpo_dagua",
    27: "nao_observado",
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

"""
This module is responsible for selecting and creating variables from the Censo dataset.
It defines lists of variables and formulas for different categories such as basic information,
demographics, household characteristics, and family relationships. These variables and formulas
are used to process and analyze census data.
"""

import os

# from src.tools.managers.db_connector import DBConnection
# from src.tools.utils.common import write_log, get_db_path

from src.databases.bronze.ibge.config import (
    manager,
)

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


basico = [
    "cd_setor",
    "situacao",
    "area_km2",
    "cd_uf",
    "cd_mun",
    "nm_bairro",
    "nm_municipio",
    "nm_uf",
    "nm_regiao",
    "nm_microregiao",
    "nm_rgint",
    "total_pessoas",
]
basico = [f"a.{col}" for col in basico]
basico_formulas = {"densidade_demografica": "a.total_pessoas / area_km2"}


demografia = [
    "qtd_moradores",
    "v01007_sxM",
    "v01008_sxF",
    "0_4_anos",
    "5_a_9_anos",
    "10_14_anos",
    "v01034_15_19_anos",
    "v01035_20_24_anos",
    "v01036_25_29_anos",
    "30_39_anos",
    "40_49_anos",
    "50_59_anos",
    "60_69_anos",
    "70_anos_ou_mais",
]
demografia = [f"b.{col}" for col in demografia]
demografia_formulas = {
    "pct_criancas_adolescentes": "SUM(b.0_4_anos + b.5_a_9_anos + b.10_14_anos)/a.total_pessoas",
    "pct_adultos": "SUM(b.v01034_15_19_anos + b.v01035_20_24_anos + b.v01036_25_29_anos +"
    " b.30_39_anos + b.40_49_anos + b.50_59_anos)/a.total_pessoas",
    "pct_idosos": "SUM(b.60_69_anos + b.70_anos_ou_mais)/a.total_pessoas",
    "pct_pia": "SUM(b.v01034_15_19_anos + b.v01035_20_24_anos + b.v01036_25_29_anos + "
    "b.30_39_anos + b.40_49_anos + b.50_59_anos)/a.total_pessoas",
    "razao_masculino_feminino": "b.v01007_sxM/b.v01008_sxF",
    "razao_dependencia_pia": "SUM(b.0_4_anos + b.5_a_9_anos + b.10_14_anos + b.60_69_anos +"
    " b.70_anos_ou_mais)/ (SUM(b.v01034_15_19_anos + b.v01035_20_24_anos + b.v01036_25_29_anos+"
    " b.30_39_anos + b.40_49_anos + b.50_59_anos))",
}


caracteristica_domicilios1 = ["v00001_dompp"]
caracteristica_domicilios1 = [f"c.{col}" for col in caracteristica_domicilios1]


parentesco = [
    "responsavel_dompp",
    "responsavel_dompp_sxM",
    "responsavel_dompp_sxF",
    "unidade_domestica_unipessoal",
    "unidade_domestica_nuclear",
    "dompp_com_responsavel_sem_conjuge_com_filhos_sxF",
    "responsavel_dompp_12_17_anos",
    "responsavel_dompp_18_24_anos",
    "responsavel_dompp_25_39_anos",
    "responsavel_dompp_40_59_anos",
    "responsavel_dompp_60_anos_ou_mais",
]
parentesco = [f"d.{col}" for col in parentesco]
parentesco_formulas = {
    "pct_dompp_chefe_sxF": "d.responsavel_dompp_sxF / d.responsavel_dompp",
    "pct_dompp_unipessoal": "d.unidade_domestica_unipessoal / c.v00001_dompp",
    "pct_dompp_nuclear": "d.unidade_domestica_nuclear / c.v00001_dompp",
    "pct_dompp_mae_solo": "d.dompp_com_responsavel_sem_conjuge_com_filhos_sxF / c.v00001_dompp",
    "pct_dompp_chefe_idoso": "d.responsavel_dompp_60_anos_ou_mais/d.responsavel_dompp",
    "pct_dompp_chefe_jovem": "SUM(d.responsavel_dompp_12_17_anos + d.responsavel_dompp_18_24_anos)"
    "/d.responsavel_dompp",
}

cor = [
    "cor_branca",
    "cor_preta",
    "cor_amarela",
    "cor_parda",
    "cor_indigena",
]

cor = [f"e.{col}" for col in cor]
cor_formulas = {
    "pct_cor_branca": "e.cor_branca / a.total_pessoas",
    "pct_cor_preta_parda": "SUM(e.cor_preta + e.cor_parda) / a.total_pessoas",
    "pct_cor_amarela": "e.cor_amarela / a.total_pessoas",
}
alfabetizacao = [
    "alfabetizadas_15_19_anos",
    "alfabetizadas_20_24_anos",
    "alfabetizadas_25_29_anos",
    "alfabetizadas_30_34_anos",
    "alfabetizadas_35_39_anos",
    "alfabetizadas_40_44_anos",
    "alfabetizadas_45_49_anos",
    "alfabetizadas_50_54_anos",
    "alfabetizadas_55_59_anos",
    "alfabetizadas_60_64_anos",
    "alfabetizadas_65_69_anos",
    "alfabetizadas_70_79_anos",
    "alfabetizadas_80_anos_ou_mais",
]
alfabetizacao = [f"f.{col}" for col in alfabetizacao]
alfabetizacao_formulas = {
    "taxa_alfabetizacao": "SUM(f.alfabetizadas_15_19_anos + f.alfabetizadas_20_24_anos + "
    "f.alfabetizadas_25_29_anos + f.alfabetizadas_30_34_anos + f.alfabetizadas_35_39_anos + "
    "f.alfabetizadas_40_44_anos + f.alfabetizadas_45_49_anos + f.alfabetizadas_50_54_anos + "
    "f.alfabetizadas_55_59_anos + f.alfabetizadas_60_64_anos + f.alfabetizadas_65_69_anos + "
    "f.alfabetizadas_70_79_anos + f.alfabetizadas_80_anos_ou_mais) / "
    "SUM(b.v01034_15_19_anos + b.v01035_20_24_anos + b.v01036_25_29_anos + "
    "b.30_39_anos + b.40_49_anos + b.50_59_anos + b.60_69_anos + b.70_anos_ou_mais)",
    "taxa_alfabetizacao_jovens": "SUM(f.alfabetizadas_15_19_anos + f.alfabetizadas_20_24_anos) / "
    "SUM(b.v01034_15_19_anos + b.v01035_20_24_anos)",
    "taxa_alfabetizacao_adultos": "SUM(f.alfabetizadas_25_29_anos + f.alfabetizadas_30_34_anos + "
    "f.alfabetizadas_35_39_anos + f.alfabetizadas_40_44_anos + "
    "f.alfabetizadas_45_49_anos + f.alfabetizadas_50_54_anos + "
    "f.alfabetizadas_55_59_anos) / "
    "SUM(b.v01036_25_29_anos + b.30_39_anos + b.40_49_anos + b.50_59_anos)",
    "taxa_alfabetizacao_idosos": "SUM(f.alfabetizadas_60_64_anos + f.alfabetizadas_65_69_anos + "
    "f.alfabetizadas_70_79_anos + f.alfabetizadas_80_anos_ou_mais) / "
    "SUM(b.60_69_anos + b.70_anos_ou_mais)",
    "pct_alfabetizacao_masculino": "SUM(f.alfabetizadas_sxM_15_19_anos + "
    "f.alfabetizadas_sxM_20_24_anos + f.alfabetizadas_sxM_25_29_anos + "
    "f.alfabetizadas_sxM_30_34_anos + "
    "f.alfabetizadas_sxM_35_39_anos + f.alfabetizadas_sxM_40_44_anos + "
    "f.alfabetizadas_sxM_45_49_anos + f.alfabetizadas_sxM_50_54_anos + "
    "f.alfabetizadas_sxM_55_59_anos + f.alfabetizadas_sxM_60_64_anos + "
    "f.alfabetizadas_sxM_65_69_anos + f.alfabetizadas_sxM_70_79_anos + "
    "f.alfabetizadas_sxM_80_anos_ou_mais) / "
    "SUM(f.v00722_sxM_15_19_anos + f.v00723_sxM_20_24_anos + f.v00724_sxM_25_29_anos + "
    "f.sxM_30_34_anos + f.sxM_35_39_anos + f.sxM_40_44_anos + "
    "f.sxM_45_49_anos + f.sxM_50_54_anos + f.sxM_55_59_anos + "
    "f.sxM_60_64_anos + f.sxM_65_69_anos + f.sxM_70_79_anos + f.sxM_80_anos_ou_mais)",
    "pct_alfabetizacao_feminino": "SUM(f.alfabetizadas_sxF_15_19_anos + "
    "f.alfabetizadas_sxF_20_24_anos + "
    "f.alfabetizadas_sxF_25_29_anos + f.alfabetizadas_sxF_30_34_anos + "
    "f.alfabetizadas_sxF_35_39_anos + f.alfabetizadas_sxF_40_44_anos + "
    "f.alfabetizadas_sxF_45_49_anos + f.alfabetizadas_sxF_50_54_anos + "
    "f.alfabetizadas_sxF_55_59_anos + f.alfabetizadas_sxF_60_64_anos + "
    "f.alfabetizadas_sxF_65_69_anos + f.alfabetizadas_sxF_70_79_anos + "
    "f.alfabetizadas_sxF_80_anos_ou_mais) / "
    "SUM(f.v00735_sxF_15_19_anos + f.v00736_sxF_20_24_anos + f.v00737_sxF_25_29_anos + "
    "f.sxF_30_34_anos + f.sxF_35_39_anos + f.sxF_40_44_anos + "
    "f.sxF_45_49_anos + f.sxF_50_54_anos + f.sxF_55_59_anos + "
    "f.sxF_60_64_anos + f.sxF_65_69_anos + f.sxF_70_79_anos + f.sxF_80_anos_ou_mais)",
    "pct_alfabetizacao_negros_pardos": "SUM()",
}


def select_and_create_variables_censo():
    """
    Select and create variables from the Censo dataset.
    """

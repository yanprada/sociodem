"""
This script retrieves data from the IBGE 2010 and
IBGE 2022 datasets and saves it as parquet files and to database.

The script contains the following functions:
- get_mun_2010: Retrieves the municipalities data from the IBGE 2010 dataset.
- get_mun_2022: Retrieves the municipalities data from the IBGE 2022 dataset.
- get_sectors_2010: Retrieves the census sectors data from the IBGE 2010 dataset.
- main: The main function that executes the script.

Note: The script assumes the existence of certain contract files and directories.
"""

import os
import zipfile
import time
import mlflow
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
import unidecode

from src.tools.managers.db_connector import DBConnection
from src.tools.managers.reader import Reader
from src.tools.managers.saver import save_parquet_decorator
from src.tools.utils.constants import STATES, CRS_GLOBAL, CRS_IBGE
from src.tools.utils.common import write_log, get_ml_flow_data, get_db_path

from src.databases.bronze.ibge.config import (
    manager,
    CONTRACTS_BRONZE,
    CONTRACTS_RAW,
    EXPERIMENT_NAME,
)

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)

mlflow.set_experiment(EXPERIMENT_NAME)


@save_parquet_decorator(medallon="bronze")
def get_dompp_per_state_2022(state, **kwargs):
    """
    Retrieves the DOMPP data for a specific state in 2022.

    Args:
        state (str): The abbreviation of the state for which the data is requested.
        **kwargs: Additional keyword arguments.

    Returns:
        pandas.DataFrame: The DOMPP data for the specified state in 2022.

    Raises:
        FileNotFoundError: If the file for the specified state is not found.

    """
    reader = Reader()
    filepath = os.path.join(
        CONTRACTS_RAW["dompp_2022"]["physicalPath"],
        "".join([state, ".zip"]),
    )
    with zipfile.ZipFile(filepath, "r") as zip_ref:
        csv_filename = zip_ref.namelist()[0]
        with zip_ref.open(csv_filename) as csv_file:
            df = reader.read_csv(csv_file, sep=";")  # type: ignore
            df = (
                df.groupby(df.columns.tolist(), as_index=False)
                .size()
                .rename(columns={"size": "count"})
                .astype({"cod_uf": "category", "cod_mun": "category"})
            )
            if "latitude" in df.columns and "longitude" in df.columns:
                df = gpd.GeoDataFrame(
                    df,
                    geometry=[Point(xy) for xy in zip(df["longitude"], df["latitude"])],
                    crs=CRS_IBGE,
                ).to_crs(CRS_GLOBAL)
            return df


def get_dompp_2022():
    """
    Retrieves the DOMPP data from the IBGE 2022 dataset.
    """
    for state in tqdm(STATES):
        with mlflow.start_run(run_name=state, nested=True):
            kwargs = {"filename": state, "contract": CONTRACTS_BRONZE["dompp_2022"]}
            df = get_dompp_per_state_2022(state, **kwargs)
            add_mlflow_metrics(df)


def upload_dompp_2022(run_name_id: str):
    """
    Uploads the DOMPP 2022 data if the specified run name ID is not present in the MLflow runs.
    Args:
        run_name_id (str): The run name ID to check in the MLflow runs.
    """
    write_log("Processing dompp data...")
    mlflow_runs_df = get_ml_flow_data(EXPERIMENT_NAME)
    if run_name_id not in mlflow_runs_df["mlflow.runName"]:
        get_dompp_2022()


def add_mlflow_metrics(df: pd.DataFrame):
    """
    Adds metrics to the mlflow run.

    Args:
        df (pd.DataFrame): The DataFrame to add metrics to.
    """
    mlflow.log_metric("num_rows", df.shape[0])


@save_parquet_decorator(medallon="bronze")
def get_ibge_data(layer_key, **kwargs):
    """
    Retrieves the data from the IBGE dataset.

    Args:
        layer_key (str): The key of the layer to use.
        **kwargs: Additional keyword arguments.

    Returns:
        pandas.DataFrame: A DataFrame containing the data.
    """
    reader = Reader()
    dfs = []
    for state in tqdm(STATES):
        with mlflow.start_run(run_name=state, nested=True):
            filepath = os.path.join(
                CONTRACTS_RAW[layer_key]["physicalPath"],
                "".join([state, ".zip"]),
            )
            df = reader.read_geofile(filepath)
            df = df.to_crs(CRS_GLOBAL)
            add_mlflow_metrics(df)
            dfs.append(df)
    dfs = pd.concat(dfs)
    return dfs


def upload_ibge_data(layer_key: str, run_name_id: str):
    """
    Uploads municipalities data for the year 2010.

    This function processes the municipalities data and checks if the data already exists.
    If the data does not exist, it calls the `get_mun_2010` function to retrieve it.
    """
    write_log(f"Processing {layer_key} data...")
    mlflow_runs_df = get_ml_flow_data(EXPERIMENT_NAME)
    if run_name_id not in mlflow_runs_df["mlflow.runName"]:
        kwargs = {"filename": "all_states", "contract": CONTRACTS_BRONZE[layer_key]}
        _ = get_ibge_data(layer_key, **kwargs)
    else:
        write_log(f"{layer_key} data already exists.")


@save_parquet_decorator(medallon="bronze")
def get_states_data(**kwargs):
    """
    Retrieves the geographical data for all states.
    """
    reader = Reader()
    dfs = []
    for state in tqdm(STATES):
        filepath = os.path.join(
            CONTRACTS_RAW["states_2022"]["physicalPath"],
            "".join([state, ".geojson"]),
        )
        df = reader.read_geofile(filepath)
        df = df.to_crs(CRS_GLOBAL)
        dfs.append(df)
    dfs = pd.concat(dfs)
    return dfs


def upload_states_2022(run_name_id: str):
    """
    Processes and uploads state data for the year 2022.
    This function reads geographical data files for each state,
    concatenates them into a single DataFrame,
    and returns the combined DataFrame. It logs the processing
    steps and uses a Reader object to read the geojson files.
    """
    write_log("Processing states data...")
    mlflow_runs_df = get_ml_flow_data(EXPERIMENT_NAME)
    if run_name_id not in mlflow_runs_df["mlflow.runName"]:
        kwargs = {"filename": "all_states", "contract": CONTRACTS_BRONZE["states_2022"]}
        _ = get_states_data(**kwargs)


@save_parquet_decorator(medallon="bronze")
def get_censo_2022_table(df_dict_censo: pd.DataFrame, filepath: str, **kwargs):
    """
    Reads a CSV file from a zip archive and returns it as a DataFrame.

    Args:
        df_dict_censo (pd.DataFrame): The DataFrame containing the dictionary for Censo 2022.
        filepath (str): The path to the zip file containing the CSV file.
        **kwargs: Additional keyword arguments.
    Returns:
        pandas.DataFrame: The DataFrame containing the data from the CSV file.
    """
    reader = Reader()
    if zipfile.is_zipfile(filepath):
        with zipfile.ZipFile(filepath, "r") as zip_ref:
            csv_filename = zip_ref.namelist()[0]
            with zip_ref.open(csv_filename) as csv_file:
                df = reader.read_csv(csv_file, sep=";", encoding="latin1", header=0)  # type: ignore
                df.columns = [
                    (
                        str(
                            df_dict_censo.query(f"variavel == '{col}'")[
                                "descricao"
                            ].iloc[0]
                        )
                        if col in df_dict_censo["variavel"].values
                        else col
                    )
                    for col in df.columns
                ]
                return df
    else:
        write_log(f"File {filepath} is not a zip file.")
    return pd.DataFrame()


def engineer_df_censo_2022(df: pd.DataFrame):
    """
    Performs data engineering on the Censo 2022 DataFrame.
    This function is a placeholder for any data engineering steps
    that need to be applied to the DataFrame.
    Args:
        df (pd.DataFrame): The DataFrame to be engineered.
    Returns:
        pd.DataFrame: The engineered DataFrame.
    """
    assert df["variavel"].nunique() == len(df), "Duplicated variables in the dictionary"
    df["descricao"] = (
        df["descricao"]
        .apply(unidecode.unidecode)
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("_+_", "_")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace(",", "")
        .str.replace("-", "")
        .str.replace("domicilios_particulares_permanentes_ocupados", "dompp")
        .str.replace("tipo_de_especie_e", "")
        .str.replace("pessoa_responsavel_pelo_domicilio", "responsavel_domicilio")
        .str.replace(
            "agua_chega_encanada_ate_dentro_da_casa_apartamento_ou_habitacao",
            "agua_encanada",
        )
        .str.replace(
            "destinacao_do_esgoto_do_banheiro_ou_sanitario_ou_buraco_para_dejecoes",
            "esgoto",
        )
        .str.replace("ligacao_a_rede_geral_de_distribuicao_de_agua", "ligacao_agua")
        .str.replace("utiliza_rios_acudes_corregos_lagos_e_igarapes", "utiliza_rios")
        .str.replace("utiliza_outra_forma_de_abastecimento_de_agua", "outra_forma_agua")
        .str.replace(
            "possui_ligacao_agua_mas_utiliza__outra_forma",
            "possui_agua_mas_outra_forma",
        )
        .str.replace("agua_chega_encanada", "agua_encanada")
        .str.replace("mas_apenas_ao_terreno", "terreno")
        .str.replace("de_zero_a_nove_anos_de_idade", "0_9_anos")
        .str.replace("quantidade", "qtd")
        .str.replace(
            "banheiro_de_uso_exclusivo_com_chuveiro_e_vaso_sanitario_existentes_no_domicilio",
            "banheiro_exclusivo",
        )
        .str.replace(
            "banheiros_de_uso_exclusivo_com_chuveiro_e_vaso_sanitario_existentes_no_domicilio",
            "banheiros_exclusivos",
        )
        .str.replace(
            "banheiro_de_uso_exclusivo_com_chuveiro_vaso_sanitario",
            "banheiro_uso_exclusivo",
        )
        .str.replace(
            "sanitario_ou_buraco_para_dejecoes_inclusive_os_localizados_no_terreno",
            "buraco_dejecoes",
        )
        .str.replace("de_uso_comum", "comum")
        .str.replace("pessoas_de_sexo", "sexo")
        .str.replace("", "")
        .str.replace("fossa_septica_ou_fossa_filtro_ligada_a_rede", "fossa")
        .str.replace(
            "fossa_septica_ou_fossa_filtro_nao_ligada_a_rede", "fossa_nao_ligada_rede"
        )
        .str.replace(
            "destinacao_do_esgoto_inexistente_pois_nao_tinham_banheiro",
            "esgoto_inexistente_sem_banheiro",
        )
        .str.replace("terreno_baldio_encosta_ou_area_publica", "terreno_baldio")
        .str.replace("principalmente_", "")
        .str.replace("janeiro_de", "jan")
        .str.replace("julho_de", "jul")
        .str.replace("da_pessoa_falecida", "defunto")
        .str.replace("mes_e_ano_de_", "")
        .str.replace("dezembro_de", "dez")
        .str.replace("_que_morava_no_dompp", "")
        .str.replace("existe_pessoa_falecida", "existe_defunto")
        .str.replace("falecimento_entre", "morte")
        .str.replace("no_periodo_de_", "")
        .str.replace("pessoa_quilombola", "quilombola")
        .str.replace("com_pelo_menos_um_morador_quilombola_", "")
        .str.replace("sexo_do_morador_responsavel", "responsavel")
        .str.replace("idade_ao_falecer_e", "idade")
        .str.replace("_e_", "_")
        .str.replace("cor_ou_raca_da_responsavel", "cor_responsavel")
        .str.replace("cor_ou_raca", "cor")
        .str.replace("idade_em_anos_ao_falecer", "idade_morte")
        .str.replace("jan_2019_a_dez_2019", "jan_dez_19")
        .str.replace("jan_2020_a_dez_2020", "jan_dez_20")
        .str.replace("jan_2021_a_dez_2021", "jan_dez_21")
        .str.replace("jan_2019_a_jul_2019", "jan_jul_19")
        .str.replace("jan_2020_a_jul_2020", "jan_jul_20")
        .str.replace("jan_2021_a_jul_2021", "jan_jul_21")
        .str.replace("jan_2022_a_jul_2022", "jan_jul_22")
        .str.replace("sexo_defunto_masculino", "sxM")
        .str.replace("sexo_defunto_feminino", "sxF")
        .str.replace("sexo_masculino_no_dompp", "sxM")
        .str.replace("sexo_feminino_no_dompp", "sxF")
        .str.replace("sexo_masculino", "sxM")
        .str.replace("sexo_feminino", "sxF")
        .str.replace("masculino", "sxM")
        .str.replace("feminino", "sxF")
        .str.replace("feminino", "sxF")
        .str.replace("domicilio", "dompp")
        .str.replace("dompp_dompp", "dompp")
        .str.replace("quilombola_morador", "quilombola")
        .str.replace("nao_existe", "sem")
        .str.replace("existe", "com")
        .str.replace("__", "_")
        .str.replace("relacao_de_parentesco_ou_de_convivencia", "relacao_parentesco")
        .str.replace(
            "conjuges_ou_companheirosas_de_sexo_diferente_"
            "ou_conjuges_ou_companheirosas_do_mesmo_sexo",
            "conjuges",
        )
        .str.replace("especie_de_unidade_domestica", "unidade_domestica")
        .str.replace("_a_", "_")
        .str.replace("conjuge_ou_companheiroa", "conjuge")
        .str.replace("com_pelo_menos_um", "minimo_um")
        .str.replace("pelo_menos_um", "minimo_um")
        .str.replace("do_", "")
        .str.replace(
            "de_sexo_diferente_nenhum_mesmo_sexo_sexo_da_responsavel",
            "sexo_diferente_responsavel",
        )
        .str.replace(
            "nenhum_de_sexo_diferente_sexo_da_responsavel",
            "mesmo_sexo_responsavel",
        )
        .str.replace(
            "minimo_um_mesmo_sexo_sexo_da_responsavel", "min_um_mesmo_sexo_responsavel"
        )
        .str.replace("mesmo_sexo_mesmo_sexo_responsavel", "mesmo_sexo_responsavel")
    )
    df["descricao"] = (
        df["descricao"]
        .str.replace(
            "dompp_minimo_um_conjuge_de_sexo_diferente_min_um_mesmo_sexo_responsavel_dompp",
            "dompp_min_um_conjuge_mesmo_sexo_e_sexo_diff_responsavel",
        )
        .str.replace("sexo_da_responsavel_dompp_sxM", "responsavel_sxM")
        .str.replace("sexo_da_responsavel_dompp_sxF", "responsavel_sxF")
        .str.replace(
            "dompp_com_responsavel_conjuges_minimo_um_filho_"
            "somente_responsavel_ou_somente_de_uma_conjuge",
            "dompp_com_responsavel_conjuges_min_um_filho",
        )
        .str.replace(
            "dompp_com_responsavel_conjuge_com_filho_de_ambos_somente_responsavel",
            "dompp_com_conjuge_com_filho_ambos",
        )
        .str.replace(
            "dompp_com_responsavel_conjuge_com_filho_de_ambos_somente_responsavel",
            "dompp_com_conjuge_com_filho_ambos",
        )
        .str.replace(
            "dompp_com_responsavel_sem_conjuge_com_filhos_e/ou_enteados_responsavel",
            "dompp_com_responsavel_sem_conjuge_com_filhos",
        )
        .str.replace("unidade_em_dompp_coletivo", "dompp_coletivo")
        .str.replace("cor_responsavel_dompp", "cor_responsavel")
        .str.replace("_utiliza_", "_")
        .str.replace("nao_tinham", "sem")
        .str.replace(
            "lixo_coletano_dompp_por_servico_de_limpeza", "coleta_lixo_servico_limpeza"
        )
        .str.replace("sem_banheiro_nem_sanitario", "sem_banheiro")
        .str.replace("_de_", "_")
        .str.replace("depositaem_cacamba_servico_limpeza", "deposita_cacamba")
    )
    df["descricao"] = (
        df["descricao"]
        .str.replace("pessoa_indigena_morador", "indigena")
        .str.replace("nao_tem", "sem")
        .str.replace("morador_indigena", "indigena")
        .str.replace("responsavel_dompp_responsavel", "responsavel_dompp")
        .str.replace("dompps_particulares_improvisados_ocupados", "dom_improvisados")
        .str.replace("dompps_coletivos_com_morador", "domicilios_coletivos")
        .str.replace(
            "dentro_estabelecimento_em_funcionamento", "dentro_estabelecimento"
        )
        .str.replace("moradores_quilombolas", "quilomb")
        .str.replace("quilombola", "quilomb")
        .str.replace(
            "outros_tipos_especie_abrigos_naturais_outras_estruturas_improvisadas",
            "estrutura_improvisada",
        )
        .str.replace(
            "estrutura_improvisada_em_logradouro_publico_exceto_tenda_ou_barraca",
            "logradouro_publico_exceto_tenda",
        )
        .str.replace(
            "dom_improvisados_dompp_com_responsavel_conjuges_minimo_um_filho",
            "dom_improvisados_responsavel_conjuges_min_um_filho",
        )
        .str.replace(
            "dom_improvisados_estrutura_nao_residencial_permanente_degradada_ou_inacabada",
            "dom_improvisados_degradado",
        )
        .str.replace(
            "asilo_ou_outra_instituicao_longa_permanencia_para_idosos",
            "asilo_idosos",
        )
        .str.replace(
            "abrigo_albergue_ou_casa_passagem_para_populacao_em_situacao_rua",
            "abrigo_pop_rua",
        )
        .str.replace(
            "abrigo_casas_passagem_ou_republica_assistencial_para_outros_grupos_vulneraveis",
            "abrigo_outros_vulneraveis",
        )
        .str.replace(
            "clinica_psiquiatrica_comunidade_terapeutica_similar",
            "clinica_psiquiatrica",
        )
        .str.replace(
            "agua_nao_chega_encanada_ao_dompp",
            "sem_agua_encanada",
        )
        .str.replace(
            "dompp_possui_ligacao_agua_mas_outra_forma_agua_encanada_terreno",
            "dompp_possui_ligacao_agua_mas_outra_forma",
        )
        .str.replace(
            "dompp_banheiro_comum_mais_um_dompp_com_chuveiro_"
            "vaso_sanitario_inclusive_os_localizados_no_terreno",
            "dompp_banheiro_comum_mais_um_dompp_no_terreno",
        )
        .str.replace(
            "um_morador",
            "um",
        )
        .str.replace(
            "inclusive_os_localizados_no_terreno",
            "no_terreno",
        )
        .str.replace(
            "com_chuveiro_vaso_sanitario",
            "com_chuveiro",
        )
        .str.replace(
            "minimo",
            "min",
        )
        .str.replace(
            "dompp_nao_banheiro_comum_mais_um_dompp_com_chuveiro_no_terreno",
            "dompp_nao_banheiro_comum_mais_um_dompp_chuveiro",
        )
        .str.replace("_morador_", "_")
        .str.replace("conjuges_ou_companheirosas", "conjuges")
        .str.replace(
            "mesmo_sexo_da_pessoa_responsavel",
            "mesmo_sexo_responsavel",
        )
        .str.replace(
            "pessoas_alfabetizadas",
            "alfabetizadas",
        )
        .str.replace(
            "_sexo_diferente_mesmo_sexo_",
            "_",
        )
    )
    df["variavel"] = df["variavel"].str.lower()
    # tem algumas colunas que tem o mesmo nome, mas com variaveis diferentes
    duplicates = df["descricao"].duplicated(keep=False)
    if duplicates.any():
        df.loc[duplicates, "descricao"] = (
            df.loc[duplicates, "variavel"] + "_" + df.loc[duplicates, "descricao"]
        )
    return df[["descricao", "variavel"]]


def get_dictionaries_map_cols_censo_2022(folder: str):
    """
    Reads the dictionary file for the census data and processes it.
    It checks for duplicate variables and normalizes the descriptions.
    Args:
        folder (str): The folder containing the dictionary file.
    """
    reader = Reader()
    df_dict_censo = reader.read_excel(
        os.path.join(folder, "dicionario_sc_geral.xlsx"),
        sheet_name="Dicionário não PCT",
    ).pipe(
        engineer_df_censo_2022,
    )
    df_dict_indigena = reader.read_excel(
        os.path.join(folder, "dicionario_sc_geral.xlsx"),
        sheet_name="Dicionário PCT - Indígenas",
    ).pipe(
        engineer_df_censo_2022,
    )
    df_dict_quilombo = reader.read_excel(
        os.path.join(folder, "dicionario_sc_geral.xlsx"),
        sheet_name="Dicionário PCT - Quilombolas",
    ).pipe(
        engineer_df_censo_2022,
    )
    df_dict_basico = reader.read_excel(
        os.path.join(folder, "dicionario_sc_geral.xlsx"),
        sheet_name="Dicionário Básico",
    ).pipe(
        engineer_df_censo_2022,
    )

    df_dict_renda = reader.read_excel(
        os.path.join(folder, "dicionario_renda.xlsx"),
        sheet_name="Dicionário Renda Responsável",
    ).pipe(
        engineer_df_censo_2022,
    )

    df_concat = pd.concat(
        [
            df_dict_censo,
            df_dict_basico,
            df_dict_renda,
            df_dict_indigena,
            df_dict_quilombo,
        ],
        ignore_index=True,
    )
    assert df_concat["variavel"].nunique() == len(
        df_concat
    ), "Duplicated variables in the dictionary"
    return df_concat


def upload_censo_2022(run_name_id: str):
    """
    Processes and uploads census data for the year 2022.
    This function reads geographical data files for each sector,
    concatenates them into a single DataFrame,
    and returns the combined DataFrame. It logs the processing
    steps and uses a Reader object to read the geojson files.
    """
    write_log("Processing censo data...")

    mlflow_runs_df = get_ml_flow_data(EXPERIMENT_NAME)
    if run_name_id not in mlflow_runs_df["mlflow.runName"]:
        folder = CONTRACTS_RAW["censo_2022"]["physicalPath"]
        df_dict_censo = get_dictionaries_map_cols_censo_2022(folder)
        for file in tqdm(os.listdir(folder), desc="Processing censo tables"):
            if not file.endswith(".zip"):
                continue
            filepath = os.path.join(folder, file)
            file = file.replace("Agregados_por_setores_", "").replace(".zip", "")
            kwargs = {
                "filename": file,
                "contract": CONTRACTS_BRONZE[f"censo_2022_{file}"],
            }
            conn = DBConnection("bronze")
            path_already_processed = get_db_path(CONTRACTS_BRONZE[f"censo_2022_{file}"])
            df_already_processed = conn.query_database(
                f"""SELECT * FROM {path_already_processed} LIMIT 1"""
            )
            conn.close()
            if df_already_processed.empty:
                _ = get_censo_2022_table(df_dict_censo, filepath, **kwargs)
            else:
                write_log(f"{file} data already exists.")


def upload_pnad_trimestral(run_name_id: str):
    """
    Processes and uploads PNAD Trimestral data.
    This function reads the PNAD Trimestral data from a CSV file,
    processes it, and checks if the data already exists in MLflow.
    Args:
        run_name_id (str): The run name ID to check in the MLflow runs.
    """
    mlflow_runs_df = get_ml_flow_data(EXPERIMENT_NAME)
    if run_name_id not in mlflow_runs_df["mlflow.runName"]:
        folder = CONTRACTS_RAW["pnad_trimestral"]["physicalPath"]
        reader = Reader()
        _ = reader.read_geofile(f"zip://{os.path.join(folder, 'pnad_trimestral.zip')}")
    # Continue from here the logic


@save_parquet_decorator(medallon="bronze")
def get_grade_estatistica_2022(filepath: str, **kwargs):
    """
    Reads a zip file containing grade estatistica data and returns it as a DataFrame.

    Args:
        filepath (str): The path to the zip file containing the grade estatistica data.
        **kwargs: Additional keyword arguments.

    Returns:
        pandas.DataFrame: The DataFrame containing the grade estatistica data.
    """
    reader = Reader()
    df = reader.read_geofile(f"zip://{filepath}")
    df = df.to_crs(CRS_GLOBAL)
    add_mlflow_metrics(df)
    return df[["id_unico", "nome_1km", "total", "total_dom", "geometry"]]


def upload_grade_estatistica(run_name_id: str) -> None:
    """
    Processes and uploads grade estatistica data for the year 2022.
    This function reads geographical data files for each grade estatistica,
    concatenates them into a single DataFrame,
    and returns the combined DataFrame. It logs the processing
    steps and uses a Reader object to read the geojson files.
    Args:
        run_name_id (str): The run name ID to check in the MLflow runs.
    """
    mlflow_runs_df = get_ml_flow_data(EXPERIMENT_NAME)
    if run_name_id not in mlflow_runs_df["mlflow.runName"]:
        folder = CONTRACTS_RAW["grade_estatistica_2022"]["physicalPath"]

        for file in tqdm(os.listdir(folder), desc="Processing grade estatistica files"):
            if not file.endswith(".zip"):
                continue
            filepath = os.path.join(folder, file)
            file = file.replace("grade_estatistica_", "").replace(".zip", "")
            kwargs = {
                "filename": file,
                "contract": CONTRACTS_BRONZE["grade_estatistica_2022"],
            }
            path_processed = os.path.join(
                CONTRACTS_BRONZE["grade_estatistica_2022"]["physicalPath"], file
            )
            if not os.path.exists(path_processed):
                _ = get_grade_estatistica_2022(filepath, **kwargs)
            else:
                write_log(f"{file} data already exists.")


def main():
    """
    The main function that executes the script.
    """
    run_date = time.strftime("%Y-%m")
    # for layer_key in [
    #     "mun_2010",
    #     "mun_2022",
    #     "sectors_2010",
    #     "sectors_2022",
    #     "districts_2010",
    #     "districts_2022",
    #     "subdistricts_2010",
    #     "subdistricts_2022",
    # ]:
    #     run_name_id = "-".join([layer_key, run_date])
    #     with mlflow.start_run(run_name=run_name_id):
    #         upload_ibge_data(layer_key, run_name_id)

    # run_name_id = "-".join(["dompp", run_date])
    # with mlflow.start_run(run_name=run_name_id):
    #     upload_dompp_2022(run_name_id)

    # run_name_id = "-".join(["states", run_date])
    # with mlflow.start_run(run_name=run_name_id):
    #     upload_states_2022(run_name_id)

    # run_name_id = "-".join(["censo_2022", run_date])
    # with mlflow.start_run(run_name=run_name_id):
    #     upload_censo_2022(run_name_id)

    # run_name_id = "-".join(["pnad_trimestral", run_date])
    # with mlflow.start_run(run_name=run_name_id):
    #     upload_pnad_trimestral(run_name_id)

    run_name_id = "-".join(["grade_estatistica", run_date])
    with mlflow.start_run(run_name=run_name_id):
        upload_grade_estatistica(run_name_id)

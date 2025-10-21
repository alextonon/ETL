"""
Voyage Voyage ETL Pipeline - Simple Version

This script runs the complete ETL pipeline:
1. Extract ... 
2. Clean and transform the data
3. Load the data into PostgreSQL database

Run with: python main.py
"""


import pandas as pd

from extract.extract_affluences import AttendanceExtractor
from extract.extract_communes import TownExtractor
from extract.extract_meteo import MeteoExtractor
from extract.extract_data_tourisme import DataTourismExtractor

from transform.transform_affluences import AttendanceTransformer
from transform.transform_communes import TownTransformer
from transform.transform_meteo import TransformMeteo
from transform.transform_data_tourisme import DataTourismTransformer

from load.load_data import get_connection_string, load_to_database_affluence, load_to_database_clusters, load_to_database_meteo, test_database_connection 
#from load.load_data import verify_data, run_sample_queries

def pipeline_meteo(df_cluster, bypass_extracts):
    """Run the Meteo ETL pipeline"""
    print("--- Starting METEO ETL Pipeline...")
    print("=" * 50)

    #### -- Meteo Extract 

    if not bypass_extracts:
        meteo_extractor = MeteoExtractor()
        df_meteo = meteo_extractor.extract_data(local=True)

        df_meteo.to_csv("data/data_extracted/df_meteo_brut.csv")

    else:
        df_meteo = pd.read_csv("data/data_extracted/df_meteo_brut.csv")

    #### -- Meteo Transform :

    transformer = TransformMeteo()

    df_mensuel = transformer.process_data(df_meteo)

    df_cluster_meteo = transformer.link_clusters_with_meteo(df_cluster, df_mensuel) 
    df_cluster_meteo.to_csv("data/data_transformed/cluster_meteo.csv", index=False) # Stockage table finale

    print("✅ Meteo ETL Pipeline completed.")

    return df_cluster_meteo

def pipeline_town(bypass_extracts):
    """Run the Town ETL pipeline"""
    print("--- Starting TOWN ETL Pipeline...")
    print("=" * 50)

    #### -- TOWN Extract
    if not bypass_extracts:
        town_extractor = TownExtractor()
        df_town = town_extractor.extract_data(["communes-france-2025.csv"])

        df_town.to_csv("data/data_extracted/df_town.csv", index=False) # Stockage intermédiaire

    else :
        df_town = pd.read_csv("data/data_extracted/df_town.csv")

    #### -- Town Transform
    Town_Transformer = TownTransformer(df_town)
    df_towncleaned = Town_Transformer.clean_data()
    df_towncleaned.to_csv("data/data_transformed/communes_france_cleaned.csv", index=False) # Stockage intermédiaire

    cluster_mapping = Town_Transformer.create_cluster_mapping() 
    cluster_mapping.to_csv("data/data_transformed/cluster_mapping.csv", index=False) # Stockage table finale

    print("✅ Town ETL Pipeline completed.")
    return df_towncleaned, cluster_mapping

def pipeline_affluences(df_towncleaned, bypass_extracts):
    """Run the Affluences ETL pipeline"""
    print("--- Starting AFFLUENCE ETL Pipeline...")
    print("=" * 50)
    
    #### -- Attendance Extract 
    if not bypass_extracts:
        Attendance_Extractor = AttendanceExtractor()

        df_capacite = Attendance_Extractor.extract_data_capacite()
        df_nb_nuitees = Attendance_Extractor.extract_data_nb_nuitees()

        df_capacite.to_csv("data/data_extracted/df_capacite.csv", index=False)
        df_nb_nuitees.to_csv("data/data_extracted/df_nb_nuitees.csv", index=False)
    
    else :
        df_capacite = pd.read_csv("data/data_extracted/df_capacite.csv")
        df_nb_nuitees = pd.read_csv("data/data_extracted/df_nb_nuitees.csv")

    #### -- Affluence Transform
    Attendance_Transformer = AttendanceTransformer()

    df_capacite = Attendance_Transformer.transform_data_capacite(df_capacite)
    df_nb_nuitees = Attendance_Transformer.transform_data_nb_nuitees(df_nb_nuitees)

    df_affluences = Attendance_Transformer.creation_dataframe_affluences(df_capacite, df_nb_nuitees)


    df_affluence_cluster = Attendance_Transformer.affluences_cluster(df_affluences, df_towncleaned)

    df_affluence_cluster.to_csv("data/data_transformed/cluster_affluence.csv", index = False) 
    print("✅ Affluence ETL Pipeline completed.")
    return df_affluence_cluster

def pipeline_datatourisme(df_town, bypass_extracts):
    """Run the DataTourism ETL pipeline"""
    print("--- Starting DATA TOURISM ETL Pipeline...")
    print("=" * 50)

    #### -- DataTourism Extract 

    if not bypass_extracts:
        DataTourism_Extractor = DataTourismExtractor()
        df_datatourisme = DataTourism_Extractor.extract_data()

        df_datatourisme.to_csv("data/data_extracted/df_datatourisme.csv")
    
    else :
        df_datatourisme = pd.read_csv("data/data_extracted/df_datatourisme.csv")

    #### -- Datatourism Transform 
    
    DataTourism_Transformer = DataTourismTransformer(df_datatourisme, df_town)
    df_datatourisme_cleaned = DataTourism_Transformer.clean_data()

    df_datatourisme_cleaned.to_csv("data/data_transformed/datatourism_cleaned.csv", index=False) 

    print("✅ DataTourism ETL Pipeline completed.")
    return df_datatourisme_cleaned

if __name__ == "__main__":
    BYPASS_EXTRACTS = False  # Set to True to skip extraction and use local files

    df_town, df_cluster = pipeline_town(BYPASS_EXTRACTS)

    print("=" * 50)

    df_meteo = pipeline_meteo(df_cluster, BYPASS_EXTRACTS)

    print("=" * 50)
    
    df_affluence = pipeline_affluences(df_town, BYPASS_EXTRACTS)

    print("=" * 50)

    df_datatourisme = pipeline_datatourisme(df_town, BYPASS_EXTRACTS)
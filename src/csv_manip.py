import os
import requests
import pandas as pd
import geopandas as gpd
import csv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ])

def create_data_directory():
    if not os.path.exists('data'):
        os.makedirs('data')
        logging.info("Répertoire 'data' créé.")


def download_and_read(url, project_name, filextension='csv', csv_separator=','):
    create_data_directory()

    if not url.startswith("http"):
        logging.error(f"L'URL fournie n'est pas valide : {url}")
        return []
    logging.info(f"Téléchargement du dataset depuis {url}") 

    response = requests.get(url)

    dataset_filename = os.path.join('data', f'all.{project_name}.{filextension}')
    if response.status_code == 200:
        with open(dataset_filename, 'wb') as f:
            f.write(response.content)
        logging.info(f"Fichier téléchargé et stocké dans {dataset_filename}")
        # use the file extension to determine the format
        if dataset_filename.endswith('.csv'):
            df = pd.read_csv(dataset_filename, sep=csv_separator, low_memory=False)
        elif dataset_filename.endswith('.xlsx'):
            df = pd.read_excel(dataset_filename, engine='openpyxl')
        elif dataset_filename.endswith('.json'):
            df = pd.read_json(dataset_filename)
        else :
            logging.error(f"Format de fichier non supporté pour {dataset_filename}")
            return []
        #sort the dataframe by all columns
        df.sort_values(by=list(df.columns), inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df
    else:
        logging.error(f"Erreur lors du téléchargement de {url}: {response.status_code}")
        return []

def delete_source_file(project_name, filextension='csv'):
    dataset_filename = os.path.join('data', f'all.{project_name}.{filextension}')
    if os.path.exists(dataset_filename):
        os.remove(dataset_filename)
        logging.info(f"Fichier {dataset_filename} supprimé.")
    else:
        logging.warning(f"Le fichier {dataset_filename} n'existe pas ou plus.")

def keep_columns(dataset, columns, export_name):
    missing_cols = [col for col in columns if col not in dataset.columns]
    if missing_cols:
        logging.warning(f"Colonnes manquantes dans le dataset : {missing_cols}")
    df_filtered = dataset[[col for col in columns if col in dataset.columns]]
    dataset_filename = os.path.join('data', f'{export_name}')
    logging.info(f"Export des colonnes {columns} vers {dataset_filename}")
    df_filtered.to_csv(dataset_filename, index=False)
    return df_filtered

def inverted_filter_by_column_values(dataset, column, value_list):
    if column in dataset.columns:
        filtered_df = dataset[~dataset[column].isin(value_list)]
        logging.info(f"Filtrage inversé des données par la colonne {column} avec les valeurs {value_list}")
        return filtered_df
    else:
        logging.error(f"La colonne {column} n'existe pas dans le dataset.")
        return dataset

def filter_by_column_values(dataset, column, value_list):
    if column in dataset.columns:
        filtered_df = dataset[dataset[column].isin(value_list)]
        logging.info(f"Filtrage des données par la colonne {column} avec les valeurs {value_list}")
        return filtered_df
    else:
        logging.error(f"La colonne {column} n'existe pas dans le dataset.")
        return dataset

def filter_by_geometry(dataset, longitude_column, latitude_column, filter_geometry_filename):
    #create a GeoDataFrame from the dataset
    if longitude_column not in dataset.columns or latitude_column not in dataset.columns:
        logging.error(f"Les colonnes {longitude_column} ou {latitude_column} n'existent pas dans le dataset.")
        return dataset
    gdf = gpd.GeoDataFrame(dataset, geometry=gpd.points_from_xy(dataset[longitude_column], dataset[latitude_column]))
    gdf.set_crs(epsg=4326, inplace=True)  # Assuming WGS84 coordinate system
    logging.info("Conversion du dataset en GeoDataFrame effectuée.")

    # read the geometry to filter by from file name
    if not os.path.exists(filter_geometry_filename):
        logging.error(f"Le fichier de géométrie {filter_geometry_filename} n'existe pas.")
        return dataset
    filter_polygon = gpd.read_file(filter_geometry_filename).geometry

    if filter_polygon.empty:
        logging.error("Le polygone de filtrage est vide.")
        return dataset
    # filter the GeoDataFrame by the geometry
    filtered_gdf = gdf[gdf.geometry.within(filter_polygon.union_all())]
    logging.info(f"Filtrage des données par la géométrie du fichier {filter_geometry_filename}")

    # check if the filtered GeoDataFrame is empty
    if filtered_gdf.empty:
        logging.warning("Aucune donnée restante après filtrage. On annule !")
        return dataset
    
    return filtered_gdf

def diff_datasets(dataset1, dataset2):
    if not isinstance(dataset1, pd.DataFrame) or not isinstance(dataset2, pd.DataFrame):
        logging.error("Les deux datasets doivent être des DataFrames.")
        return None
    diff = pd.concat([dataset1, dataset2]).drop_duplicates(keep=False)
    if not diff.empty:
        logging.info(f"Il y a des différences entre les deux datasets, {len(diff)} objets à analyser.")
    else:
        logging.info("Aucune différence entre les deux datasets.")
    return diff



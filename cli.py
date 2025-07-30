import logging
import json
import sys

from src.csv_manip import download_and_read, keep_columns, inverted_filter_by_column_values, filter_by_column_values, filter_by_geometry, diff_datasets, delete_source_file


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ])

def run_pipeline_from_config(config_path):
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # Map function names to actual functions
    function_map = {
        "keep_columns": keep_columns,
        "filter_by_column_values": filter_by_column_values,
        "inverted_filter_by_column_values": inverted_filter_by_column_values,
        "filter_by_geometry": filter_by_geometry,
        "diff_datasets": diff_datasets,
    }

    if "source" in config and "url" in config["source"]:
        url = config["source"]["url"]
        csv_separator = config["source"].get("csv_separator", ',')
        project_name = config.get("project_name", "dataset")
        df = download_and_read(url, project_name, filextension='csv', csv_separator=csv_separator)   
    else:
        logging.error("Aucune source de données fournie dans le fichier de configuration.")
        return  

    
    for transformation in config.get("transformations", []):
        steps = transformation.get("steps", [])
        last_output = df

        for idx, step in enumerate(steps):
            func_name = step["function"]
            args = step.get("args", {}).copy()

            if "dataset" not in args:
                if last_output is not None:
                    args["dataset"] = last_output
                else:
                    logging.error("Aucun dataset disponible pour la première étape.")
                    return

            last_output = function_map[func_name](**args)
    
    delete_source_file(project_name, filextension='csv')

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Usage: python config.py <config_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    run_pipeline_from_config(config_path)
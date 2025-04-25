
from airflow.models.baseoperator import BaseOperator
import zipfile
import os
import requests

import pandas as pd
from io import BytesIO


class UnzipOperator(BaseOperator):
    def __init__(self, input_path: str, output_path: str, **kwargs):
        super().__init__(**kwargs)
        self.input_path = input_path
        self.output_folder = output_path

    def execute(self, context):
        os.makedirs(self.output_folder, exist_ok=True)

        # Extraire le fichier ZIP
        with zipfile.ZipFile(self.input_path, 'r') as zip_ref:
            zip_ref.extractall(self.output_folder)  # Extraire dans un dossier temporaire

            # Identifier le fichier Excel extrait (supposons qu'il y ait un seul fichier .xlsx)
            extracted_files = zip_ref.namelist()

            print(extracted_files)
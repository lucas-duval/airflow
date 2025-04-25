from airflow.models.baseoperator import BaseOperator
import os
import requests
import gzip

import pandas as pd
from io import BytesIO


class UngzOperator(BaseOperator):
    def __init__(self, input_path: str, output_path: str, **kwargs):
        super().__init__(**kwargs)
        self.input_path = input_path
        self.output_folder = output_path

    def execute(self, context):
        os.makedirs(self.output_folder, exist_ok=True)

        # Extraire le fichier ZIP
        with gzip.open('file.txt.gz', 'rb') as f:
            file_content = f.read()  # Extraire dans un dossier temporaire

            
            extracted_files = file_content

            print(extracted_files)
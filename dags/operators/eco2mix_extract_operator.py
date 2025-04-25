
from airflow.models.baseoperator import BaseOperator
import zipfile
import os
import requests

import pandas as pd
from io import BytesIO


class Eco2MixExtractorOperator(BaseOperator):
    def __init__(self, date: str, output_path: str, **kwargs):
        super().__init__(**kwargs)
        self.date = date
        self.output_path = output_path

    def execute(self, context):
        """Télécharge, décompresse et convertit le fichier Excel en CSV"""
        url = f"https://eco2mix.rte-france.com/curves/eco2mixDl?date={self.date}"
        response = requests.get(url)

        if response.status_code == 200:
            # Créer un répertoire de sortie si nécessaire
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

            # Enregistrer le fichier ZIP téléchargé
            zip_file_path = '/tmp/eco2mix_data.zip'
            with open(self.output_path, 'wb') as f:
                f.write(response.content)

            print(f"✅ Fichier téléchargé : {self.output_path}")
        else:
            print(f"❌ Erreur de téléchargement: {response.status_code}, {response.text}")

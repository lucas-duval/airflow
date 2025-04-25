from airflow.models.baseoperator import BaseOperator
import os
import requests

class MeteoExtractorOperator(BaseOperator):
    def __init__(self, output_path: str, **kwargs):
        super().__init__(**kwargs)
        self.output_path = output_path
        self.url = "https://www.data.gouv.fr/fr/datasets/r/c1265c02-3a8e-4a28-961e-26b2fd704fe8"

    def execute(self, context):
        """Télécharge le fichier CSV du bilan électrique depuis Enedis et l'enregistre localement"""
        response = requests.get(self.url)

        if response.status_code == 200:
            # Créer un répertoire de sortie si nécessaire
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

            # Enregistrer le contenu CSV
            with open(self.output_path, 'wb') as f:
                f.write(response.content)

            print(f"✅ Fichier Enedis téléchargé : {self.output_path}")
        else:
            print(f"❌ Erreur de téléchargement: {response.status_code}, {response.text}")

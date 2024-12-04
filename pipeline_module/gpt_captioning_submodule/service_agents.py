
import os
import json
import vertexai
from vertexai.generative_models import GenerativeModel
import requests

class ServiceAgents:
    def __init__(self):
        self.gcp_project_id = None
        self.gcp_region = None
        self.openai_api_key = None
        self.init_gcp_agent()
        self.init_openai_agent()

    def init_gcp_agent(self):
        # Load GCP credentials
        gac_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not gac_path:
            raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
        
        with open(gac_path, "r") as key_file:
            gcp_credentials = json.load(key_file)
            self.gcp_project_id = gcp_credentials.get("project_id")
            self.gcp_region = gcp_credentials.get("region", "us-central1")

        vertexai.init(project=self.gcp_project_id, location=self.gcp_region)

    def init_openai_agent(self):
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise EnvironmentError("OPENAI_API_KEY environment variable not set.")

    def get_vertex_model(self, model_name: str) -> GenerativeModel:
        return GenerativeModel(model_name)

    def get_openai_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.openai_api_key}",
            "Content-Type": "application/json"
        }

"""One-time setup: creates a Vertex AI Search (Discovery Engine) Data Store in GCP.

Run once before starting the sync job. The resulting Data Store ID is what you
attach to your Gemini Enterprise app.
"""
import os

from dotenv import load_dotenv
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import AlreadyExists
from google.cloud import discoveryengine_v1 as discoveryengine

load_dotenv()

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCP_LOCATION = os.environ.get("GCP_LOCATION", "global")
DATASTORE_ID = os.environ["DATASTORE_ID"]
DATASTORE_DISPLAY_NAME = os.environ.get("DATASTORE_DISPLAY_NAME", DATASTORE_ID)


def create_data_store() -> None:
    client_options = (
        ClientOptions(api_endpoint=f"{GCP_LOCATION}-discoveryengine.googleapis.com")
        if GCP_LOCATION != "global"
        else None
    )
    client = discoveryengine.DataStoreServiceClient(client_options=client_options)
    parent = client.collection_path(
        project=GCP_PROJECT_ID, location=GCP_LOCATION, collection="default_collection"
    )

    data_store = discoveryengine.DataStore(
        display_name=DATASTORE_DISPLAY_NAME,
        industry_vertical=discoveryengine.IndustryVertical.GENERIC,
        solution_types=[discoveryengine.SolutionType.SOLUTION_TYPE_SEARCH],
        # Structured Databricks rows only, no unstructured document content.
        content_config=discoveryengine.DataStore.ContentConfig.NO_CONTENT,
    )

    try:
        operation = client.create_data_store(
            parent=parent,
            data_store=data_store,
            data_store_id=DATASTORE_ID,
        )
        result = operation.result()
        print(f"Created data store: {result.name}")
    except AlreadyExists:
        print(f"Data store '{DATASTORE_ID}' already exists, skipping creation.")


if __name__ == "__main__":
    create_data_store()

import os
import json
import time
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest

# Azure configuration
STORAGE_ACCOUNT = "storage_account_name"  # Replace with your storage account name
CONTAINER_NAME = "container_name"  # Replace with your container name
DOCUMENT_INTELLIGENCE_NAME = "document_intelligence_name"  # Replace with your Document Intelligence resource name
MODEL_ID = "prebuilt-invoice" # Change if need to use a different model: https://learn.microsoft.com/en-us/azure/ai-services/document-intelligence/model-overview#model-analysis-features

# Initialize clients
storage_account_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/"
document_intelligence_endpoint = f"https://{DOCUMENT_INTELLIGENCE_NAME}.cognitiveservices.azure.com/"
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=credential)
document_intelligence_client = DocumentIntelligenceClient(endpoint=document_intelligence_endpoint, credential=credential)


def analyze_blob(blob_name):
    blob_url = f"{storage_account_url}{CONTAINER_NAME}/{blob_name}"

    print(f"Analyzing blob: {blob_name}")
    poller = document_intelligence_client.begin_analyze_document('prebuilt-invoice', AnalyzeDocumentRequest(url_source=blob_url))
    result = poller.result()

    # Save results as JSON
    output_blob_name = f"{os.path.splitext(blob_name)[0]}_result.json"
    output_blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=output_blob_name)
    result_json = { "analyzed_document": result.as_dict() }
    output_blob_client.upload_blob(json.dumps(result_json, indent=2), overwrite=True)
    print(f"Results saved to: {output_blob_name}")

def main():
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blobs = list(container_client.list_blobs())
    
    # Create a set of existing result files
    existing_results = {blob.name for blob in blobs if blob.name.endswith("_result.json")}
    
    for blob in blobs:
        if not blob.name.endswith("_result.json"):  # Skip already processed files
            # Check if the corresponding result file exists
            output_blob_name = f"{os.path.splitext(blob.name)[0]}_result.json"
            
            if output_blob_name not in existing_results:
                analyze_blob(blob.name)                
                time.sleep(5)
            else:
                print(f"Skipping {blob.name}, result file already exists")

if __name__ == "__main__":
    main()
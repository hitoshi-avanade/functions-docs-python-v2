import azure.functions as func
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="HttpExample")
def HttpExample(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
        )
    

from azure.storage.blob import BlobServiceClient
import fitz  # PyMuPDF
import os, json, requests, time, functools, base64, sys
from io import BytesIO
from openai import AzureOpenAI

sys.stdout.reconfigure(encoding='utf-8')

STORAGE_CONNECTION_STRING = os.getenv('ConnectionString_AzureDataFileStorage')
container_name  = os.getenv('DataStorage_ContainerName')
AZURE_SEARCH_SERVICE_URL = os.getenv('AZURE_SEARCH_SERVICE_URL')
index_name  = os.getenv('AZURE_SEARCH_INDEX_NAME')
AZURE_SEARCH_API_KEY = os.getenv('AZURE_SEARCH_API_KEY')
AZURE_OPENAI_EMBEDDING_MODEL = os.getenv('AZURE_OEPNAI_EMBEDDING_MODEL')
blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
last_file_list_blob = "last_file_list.json"
api_version = os.getenv('AI_SEARCH_API_VERSION')

# https://qiita.com/mahiya/items/1d9aaab3c242fd31bc8b
# Azure AI Search のスキルセットで PDF ファイルを読み込んでチャンク分割してベクトル化する

client = AzureOpenAI(
    api_key = os.getenv("AZURE_OPENAI_API_KEY"),  
    api_version = "2024-02-01",
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
)


# 429 too many request 回避用にリトライデコレータを作成する
def exponential_backoff(retries=5, backoff_in_seconds=4, max_backoff_in_seconds=128):
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    logging.info(f"Attempt {attempt + 1} of {retries}...")
                    response = func(*args, **kwargs)
                    return response
                except Exception as e:  # Modify this line
                    wait = min(max_backoff_in_seconds, backoff_in_seconds * (2 ** attempt))
                    logging.info(f"Request failed with {e}, retrying in {wait} seconds...")
                    time.sleep(wait)
                    attempt += 1
        return wrapper_retry
    return decorator_retry

def get_blob_list():
    container_client = blob_service_client.get_container_client(container_name)
    return [blob.name for blob in container_client.list_blobs()]

def extract_text_from_pdf(blob):
    blob_client = blob_service_client.get_blob_client(container_name, blob)
    pdf_data = blob_client.download_blob().readall()
    pdf_document = fitz.open(stream=pdf_data, filetype="pdf")
    text = ""

    try:
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            text += page.get_text()
        
        text2 = text.replace('\n', ' ')
        print(f'Extracted text: {text2}')
    except Exception as e:
        logging.error(f"Error extracting text: {str(e)}")
        text = "error"
    return text


@exponential_backoff()
def get_embedding(text):
    response = client.embeddings.create(
        input=text, 
        model=AZURE_OPENAI_EMBEDDING_MODEL
    )
    return response.data[0].embedding


def update_search_index(documents):
    url = f"{AZURE_SEARCH_SERVICE_URL}/indexes/{index_name}/docs/index?api-version={api_version}"
    headers = {
        "Content-Type": "application/json",
        "api-key": AZURE_SEARCH_API_KEY
    }
    data = {
        "value": documents
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error: {e.response.text}")
        raise


def encode_document_key(key):
    return base64.urlsafe_b64encode(key.encode()).decode()

def load_last_file_list():
    try:
        blob_client = blob_service_client.get_blob_client(container_name, last_file_list_blob)
        blob_data = blob_client.download_blob().readall().decode('utf-8')
        return json.loads(blob_data)
    except Exception as e:
        logging.info(f"Failed to load last file list: {str(e)}")
        return []


def save_current_file_list(file_list):
    blob_client = blob_service_client.get_blob_client(container_name, last_file_list_blob)
    blob_data = json.dumps(file_list, ensure_ascii=False).encode('utf-8')
    blob_client.upload_blob(BytesIO(blob_data), overwrite=True)

def get_updated_files():
    current_file_list = get_blob_list()
    last_file_list = load_last_file_list()

    # last_file_list.jsonを除外
    current_file_list = [f for f in current_file_list if f != last_file_list_blob]

    new_files = list(set(current_file_list) - set(last_file_list))
    deleted_files = list(set(last_file_list) - set(current_file_list))
    updated_files = list(set(current_file_list) & set(last_file_list))  # Assuming no way to detect updates without metadata
    
    save_current_file_list(current_file_list)
    
    return new_files, deleted_files, updated_files

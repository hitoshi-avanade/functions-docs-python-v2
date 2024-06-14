import azure.functions as func
import logging


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="HttpExample")
def HttpExample(req: func.HttpRequest) -> func.HttpResponse:
    # ここにimportを記載しないと、Azure portalからFunctionsが見えなくなる
    from func_module import get_updated_files, encode_document_key, extract_text_from_pdf, get_embedding, update_search_index
    import time

    logging.info('Python HTTP trigger function processed a request.')

    try:
        new_files, deleted_files, updated_files = get_updated_files()
        
        logging.info(f"New files: {new_files}")
        logging.info(f"Deleted files: {deleted_files}")
        logging.info(f"Updated files: {updated_files}")
        print(f"New files: {new_files}\nDeleted files: {deleted_files}\nUpdated files: {updated_files}")

        documents = []

        # Handle new and updated files
        for filename in new_files + updated_files:
            text = extract_text_from_pdf(filename)
            document = {
                "@search.action": "mergeOrUpload",
                "id": encode_document_key(filename),
                "filename": filename,
                "embedding": get_embedding(text),
                "content": text
            }
            documents.append(document)
            # embeddingで429発生しないように待機する
            time.sleep(2)
        
        if documents:
            update_search_index(documents)
        
        return func.HttpResponse("Index updated successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

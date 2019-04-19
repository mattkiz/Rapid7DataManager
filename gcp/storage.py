from google.cloud import storage

storage_client = storage.Client()

HTTP_RAW_DATA_BUCKET = storage_client.bucket("http_raw_data")


def save_data_to_cloud(encoded_str, name, testing=False):
    blob = HTTP_RAW_DATA_BUCKET.blob(name)
    if not testing:
        blob.upload_from_string(encoded_str)
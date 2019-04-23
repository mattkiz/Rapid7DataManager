from google.cloud import storage

storage_client = storage.Client()

HTTP_RAW_DATA_BUCKET = storage_client.bucket("http_raw_data")

DEBUG=True

def save_data_to_cloud(encoded_str, name, testing=DEBUG):
    blob = HTTP_RAW_DATA_BUCKET.blob(name)
    if not testing:
        blob.upload_from_string(encoded_str)


def save_data_to_cloud_multi(data, testing=DEBUG):
    for d in data:
        b = HTTP_RAW_DATA_BUCKET.blob(d[1])
        if not testing:
            b.upload_from_string(d[0])
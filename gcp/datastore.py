from google.cloud import datastore
from ipaddress import IPv4Address

client = datastore.Client()


def create_url_metadata(meta_data, source="rapid7", testing=False):
    with client.transaction():
        key = client.key("url_metadata")
        key = client.allocate_ids(key, 1)[0]
        e = datastore.Entity(key)
        meta_data["source"] = source
        e.update(meta_data)
        if not testing:
            client.put(e)
        return key.flat_path

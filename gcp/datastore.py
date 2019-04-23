from google.cloud import datastore
from ipaddress import IPv4Address
import threading


DEBUG=True
client = datastore.Client()


def create_url_metadata(meta_data, source="rapid7", testing=DEBUG):
    with client.transaction():
        key = client.key("url_metadata")
        key = client.allocate_ids(key, 1)[0]
        e = datastore.Entity(key)
        meta_data["source"] = source
        e.update(meta_data)
        if not testing:
            client.put(e)
        return key.flat_path

# __key_lock = threading.Lock()

def create_url_metadata_multi(meta_datas, source="rapid7", testing=DEBUG):
    with client.transaction():
        key = client.key("url_metadata")
        # __key_lock.acquire()
        keys = client.allocate_ids(key, len(meta_datas))
        # __key_lock.release()
        entities = []
        for i in range(len(meta_datas)):
            e = datastore.Entity(keys[i])
            e.update(meta_datas[i])
            entities.append(e)
        if not testing:
            client.put_multi(entities)
        return list(map(lambda x: x.flat_path, keys))

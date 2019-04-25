from google.cloud import datastore
import google
from ipaddress import IPv4Address
import threading


DEBUG=False
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

def create_url_metadata_multi(meta_datas, testing=DEBUG, excluded_indicies=None):
    with client.transaction():
        key = client.key("url_metadata")
        keys = client.allocate_ids(key, len(meta_datas))
        entities = []
        for i in range(len(meta_datas)):
            e = datastore.Entity(keys[i], exclude_from_indexes=excluded_indicies)
            # meta_datas[i].pop("data", None)
            e.update(meta_datas[i])
            entities.append(e)
        if not testing:
            try:
                client.put_multi(entities)
            except Exception:
                for e in entities:
                    try:
                        client.put(e)
                    except Exception:
                        continue
        return list(map(lambda x: x.flat_path, keys))

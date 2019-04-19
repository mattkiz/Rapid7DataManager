import sys
import queue
import threading
import subprocess
from gcp.datastore import *
from gcp.storage import *
from rapid7 import *
NUM_THREADS=4

i = 0
# n = 0
stat_lock = threading.Lock()
start_timer = None
# last_timer = None

def save_chunk(q:queue.Queue, stop:threading.Event):
    global stat_lock, i, start_timer
    while not stop.is_set() or not q.empty():
        if q.empty():
            continue
        chunk = q.get(block=True)
        if chunk is b"":
            continue
        obj_raw = json.loads(chunk)
        data = obj_raw["data"]
        del obj_raw["data"]
        obj_raw["source"] = "rapid7"
        key = create_url_metadata(obj_raw, testing=True)
        storage_name = "port_{1}/http_raw_data_{0}".format(key[1], obj_raw["port"])
        save_data_to_cloud(data, storage_name, testing=True)
        stat_lock.acquire()
        if i % 1000 == 0:
            print("{0} requests processed.".format(i))
            print("{0:2} seconds taken".format(time.time() - start_timer))
        i += 1
        stat_lock.release()

def unpack_file(fileobj):
    global start_timer
    download_url = fileobj["url"]
    p = subprocess.Popen(["curl {0} | zcat".format(download_url)],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    q = queue.Queue()
    stop = threading.Event()
    start_timer = time.time()
    for i in range(NUM_THREADS):
        t = threading.Thread(target=save_chunk, args=[q, stop])
        t.start()
    while not p.poll():
        q.put(p.stdout.readline(), block=True)
    print("Done unpacking...")


def main():
    filename = sys.argv[1]
    fileobj = find_download_url(filename)
    unpack_file(fileobj)

if __name__ == '__main__':
    main()


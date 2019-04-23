import sys
import queue
import threading
import subprocess
import collections
import re

from gcp.datastore import *
from gcp.storage import *
from rapid7 import *

NUM_THREADS=3

i = 0
n = 0
stat_lock = threading.Lock()
start_timer = None
max_chunks = 100
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
            print(q.qsize())
            print("{0} requests processed.".format(i))
            print("{0:2} seconds taken".format(time.time() - start_timer))
        i += 1
        stat_lock.release()

def get_datas(x):
    return x.pop("data", None)


def save_chunk_sem(q:collections.deque, stop:threading.Event, sem:threading.Semaphore, run_info):
    global stat_lock, i, start_timer, max_chunks, n
    while not stop.is_set():
        sem.acquire()
        chunks = collections.deque()
        if len(q) == 0:
            sem.release()
            continue
        curr = q.popleft()
        c = 0
        while len(q) != 0 and c < max_chunks:
            chunks.append(curr)
            curr = q.popleft()
            c +=1
        sem.release()

        json_objs = collections.deque(map(json.loads, chunks))
        data = collections.deque(map(lambda x: get_datas(x), json_objs))
        keys = create_url_metadata_multi(json_objs)
        zipped_data = zip(data, map(lambda x: "port_{1}/{2}_raw_data_{0}".format(x[1], run_info["port"], run_info["http_https"]),
                                    keys))
        save_data_to_cloud_multi(zipped_data)
        stat_lock.acquire()
        i += len(chunks)
        if i > n:
            print(len(q))
            print("{0} requests processed.".format(i))
            print("{0:2} seconds taken".format(time.time() - start_timer))
            n += 1000
        stat_lock.release()

def unpack_file(fileobj):
    global start_timer
    download_url = fileobj["url"]
    p = subprocess.Popen(["curl {0} | zcat".format(download_url)],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    q = queue.Queue()
    l = collections.deque()
    run_info = {}
    run_info["updated_at"] = fileobj["updated_at"]
    run_info["port"] = int(re.search(r"\_(\d+)\.json", fileobj["name"]).group(1))
    run_info["http_https"] = re.search(r"\/sonar\.(.+)\/", download_url).group(1)
    semaphore = threading.Semaphore()
    stop = threading.Event()
    start_timer = time.time()
    for i in range(NUM_THREADS):
        t = threading.Thread(target=save_chunk_sem, args=[l, stop, semaphore, run_info])
        # t = threading.Thread(target=save_chunk, args=[q, stop])
        t.start()
    while not p.poll():
        q.put(p.stdout.readline())
    while not q.empty():
        continue
    stop.set()
    print("Done unpacking...")


def main():
    filename = sys.argv[1]
    fileobj = find_download_url(filename)
    unpack_file(fileobj)

if __name__ == '__main__':
    main()


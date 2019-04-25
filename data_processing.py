import sys
import queue
import threading
import multiprocessing
import subprocess
import collections
import re
import signal
from gcp.datastore import *
from gcp.storage import *
from rapid7 import *
import cloud_upload


NUM_THREADS=8

i = 0
n = 0
stat_lock = threading.Lock()
start_timer = None
max_chunks = 5000


def get_datas(x):
    return x.pop("data", None)


def save_chunk_sem(q:collections.deque, stop:threading.Event, sem:threading.Semaphore, run_info, storage_q):
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
        c = 0
        size = len(chunks)
        curr_chunks = collections.deque()
        while len(chunks) > 0:
            if c == 499:
                json_objs = collections.deque(map(json.loads, curr_chunks))
                data = collections.deque(map(lambda x: get_datas(x), json_objs))
                keys = create_url_metadata_multi(json_objs, excluded_indicies=run_info["excluded_indices"])
                zipped_data = zip(data, map(lambda x: x[1], keys))
                for z in zipped_data:
                    storage_q.put(bytes("{0},{1}\n".format(z[0], z[1]), encoding="utf-8"))
                curr_chunks = collections.deque()
                c = 0
            curr_chunks.append(chunks.popleft())
            c +=1
        json_objs = collections.deque(map(json.loads, chunks))
        data = collections.deque(map(lambda x: get_datas(x), json_objs))

        keys = create_url_metadata_multi(json_objs, excluded_indicies=run_info["excluded_indices"])
        zipped_data = zip(data, map(lambda x: x[1], keys))
        for z in zipped_data:
            storage_q.put(bytes("{0},{1}\n".format(z[0], z[1]), encoding="utf-8"))
        del json_objs
        del data
        del keys
        stat_lock.acquire()
        i += size
        del chunks
        if i > n:
            print(len(q))
            print("{0} requests processed.".format(i))
            print("{0:2} seconds taken".format(time.time() - start_timer))
            n += 1000
        stat_lock.release()
    print("Thread is done!")



def unpack_file(fileobj):
    global start_timer
    download_url = fileobj["url"]
    run_info = {}
    run_info["updated_at"] = fileobj["updated_at"]
    run_info["port"] = int(re.search(r"\_(\d+)\.json", fileobj["name"]).group(1))
    run_info["http_https"] = re.search(r"\/sonar\.(.+)\/", download_url).group(1)
    run_info["excluded_indices"] = ["host", "path", "subject", "vhost"]
    p = subprocess.Popen(["curl {0} --keepalive-time 2 | gunzip -c".format(download_url)],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    storage_q = multiprocessing.Queue()
    storage_stop = multiprocessing.Event()
    storage_p = multiprocessing.Process(target=cloud_upload.main, args=[storage_q, storage_stop, run_info["port"], run_info["http_https"]])
    # storage_p = threading.Thread(target=cloud_upload.main, args=[storage_q, storage_stop, run_info["port"], run_info["http_https"]])
    storage_p.start()
    q = collections.deque()

    semaphore = threading.Semaphore()
    stop = threading.Event()
    start_timer = time.time()
    threads = []
    for i in range(NUM_THREADS):
        t = threading.Thread(target=save_chunk_sem, args=[q, stop, semaphore, run_info, storage_q])
        t.start()
        threads.append(t)
    while p.returncode != 0:
        line = p.stdout.readline()
        if line == b"":
            break
        q.append(line)
    print("curl gzip is done...")
    while True:
        semaphore.acquire()
        if len(q) == 0:
            semaphore.release()
            break
        semaphore.release()
        time.sleep(0.001)
    stop.set()
    for t in threads:
        t.join()
    print("sending stop to storage...")
    storage_stop.set()
    storage_p.join()
    print("Done unpacking...")


def main():
    filename = sys.argv[1]
    fileobj = find_download_url(filename)
    unpack_file(fileobj)

if __name__ == '__main__':
    main()


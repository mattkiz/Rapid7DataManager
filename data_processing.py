import sys
import multiprocessing
import subprocess
import collections
import re
from gcp.datastore import *
from rapid7 import *
import cloud_upload
import datetime
import queue

NUM_THREADS=4

i = 0
n = 0
stat_lock = threading.Lock()
start_timer = None
max_chunks = 5000


def get_datas(x):
    return x.pop("data", None)


def attach_more_metadata(x, other_metadata):
    for k, v in other_metadata.items():
        x[k] = v
    return x


def save_chunk_sem(q:queue.Queue, stop:threading.Event, lock:threading.Lock, run_info, namespace, storage_q):
    global stat_lock, i, start_timer, max_chunks, n
    while not stop.is_set():
        chunks = collections.deque()
        lock.acquire()
        for j in range(max_chunks):
            try:
                chunks.append(q.get(block=False).decode("utf-8"))
                q.task_done()
            except queue.Empty:
                break
        lock.release()
        c = 0
        size = 0
        curr_chunks = collections.deque()
        while True:
            if c == 499:
                json_objs = map(json.loads, curr_chunks)
                data = map(lambda x: get_datas(x), json_objs)

                json_objs = map(lambda x: attach_more_metadata(x, run_info["other_metadata"]), json_objs)
                json_objs = collections.deque(json_objs)

                keys = create_url_metadata_multi(json_objs, excluded_indicies=run_info["excluded_indices"])
                zipped_data = zip(data, map(lambda x: x[1], keys))
                for z in zipped_data:
                    print(z)
                    storage_q.put("{0},{1}\n".format(z[0], z[1]))
                curr_chunks = collections.deque()
                size += c
                c = 0
            try:
                entity = chunks.popleft()
            except IndexError:
                break
            curr_chunks.append(entity)
            c +=1
        size += c
        json_objs = collections.deque(map(json.loads, chunks))
        data = collections.deque(map(lambda x: get_datas(x), json_objs))
        keys = create_url_metadata_multi(json_objs, excluded_indicies=run_info["excluded_indices"])
        zipped_data = zip(data, map(lambda x: x[1], keys))
        for z in zipped_data:
            storage_q.put("{0},{1}\n".format(z[0], z[1]))
        del json_objs
        del data
        del keys
        stat_lock.acquire()
        namespace.i += size
        del chunks
        if namespace.i > namespace.n:
            print(q.qsize())
            print("{0} requests processed.".format(namespace.i))
            print("{0:2} seconds taken".format(time.time() - namespace.start_timer))
            namespace.n += 10000
        stat_lock.release()
    print("Thread is done!")



def unpack_file(fileobj):
    global start_timer
    download_url = fileobj["url"]
    # Creating info from the metadata to be stored on the entity
    run_info = {}
    run_info["updated_at"] = fileobj["updated_at"]
    run_info["other_metadata"] = {"updated_at": run_info["updated_at"], "extracted_on": datetime.date.today().strftime("%Y-%m-%d"),
                                  "source": "rapid7"}
    run_info["port"] = int(re.search(r"\_(\d+)\.json", fileobj["name"]).group(1))
    run_info["http_https"] = re.search(r"\/sonar\.(.+)\/", download_url).group(1)
    run_info["excluded_indices"] = ["host", "path", "subject", "vhost", "extracted_on"]

    # Starting curl and unzip subprocess
    p = subprocess.Popen(["curl {0} --keepalive-time 2 | gunzip -c".format(download_url)],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    # Starting child process to upload to storage
    storage_q = multiprocessing.JoinableQueue()
    storage_stop = multiprocessing.Event()
    storage_p = multiprocessing.Process(target=cloud_upload.main,
                                        args=[storage_q, storage_stop, run_info])
    storage_p.start()

    # Creating the entity threads
    manager = multiprocessing.Manager()
    Global = manager.Namespace()
    Global.i = 0
    Global.n = 0

    # Global.storage_queue = storage_q
    q = multiprocessing.JoinableQueue()
    semaphore = multiprocessing.Lock()
    stop = multiprocessing.Event()
    Global.start_timer = time.time()
    threads = []
    for i in range(NUM_THREADS):
        t = multiprocessing.Process(target=save_chunk_sem, args=[q, stop, semaphore, run_info, Global, storage_q])
        t.start()
        threads.append(t)
    line_c = 0

    # Main loop for reading the curl process
    while p.returncode != 0:
        line = p.stdout.readline()
        if line == b"":
            break
        line_c+=1
        q.put(line)  # Never block because the queue has unlimited storage space
    print("curl gzip is done...")
    print("lines read: {0}".format(line_c))
    q.join()
    # Stopping and joining entity threads
    stop.set()
    for t in threads:
        t.join()
    print(Global.i)
    # stopping the storage process
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


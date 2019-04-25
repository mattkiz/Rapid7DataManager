from gcp import storage
from collections import deque
import threading
import gzip
import shutil
import os
import multiprocessing
import queue
import time
n = 0
stat_lock = threading.Lock()
MaxEntries = 10000
filenum = 0
files_toupload = []
NUM_UPLOAD_THREADS = 3
name_lock = threading.Lock()


def upload_thread(filename, port, updated_at):
    compressed_filename = '{0}.gz'.format(filename)
    with open(filename, 'rb') as f_in:
        f_out = gzip.open(compressed_filename, 'wb+')
        shutil.copyfileobj(f_in, f_out)
    f_out.close()
    storage.save_file_to_cloud(compressed_filename, "port_{0}_{1}/{2}".format(port, updated_at, os.path.basename(compressed_filename)))
    os.remove(filename)
    os.remove(compressed_filename)


def csv_save(in_q, prefix, stop, port, updated_at):
    global filenum, n, stat_lock
    name_lock.acquire()
    filename = "output/{0}{1}.csv".format(prefix, filenum)
    filenum += 1
    name_lock.release()
    f = open(filename, "w")
    i = 0
    threads = []
    while True:
        try:
            item = in_q.get(block=False)
        except queue.Empty:
            if stop.is_set():
                break
            continue
        if i > MaxEntries:
            f.close()
            t = threading.Thread(target=upload_thread, args=[filename, port, updated_at])
            t.start()
            threads.append(t)
            name_lock.acquire()
            filename = "output/{0}{1}.csv".format(prefix, filenum)
            filenum += 1
            name_lock.release()
            f = open(filename, "w")
            i = 0
        i+=1
        f.write(item.decode("utf-8"))
        stat_lock.acquire()
        n+=1
        stat_lock.release()
        in_q.task_done()
    print("got stop... exiting other threads...")
    f.close()
    upload_thread(filename, port, updated_at)
    for t in threads:
        t.join()


def main(data_q, stop:multiprocessing.Event, run_info):
    global filenum
    port = run_info["port"]
    updated_at = run_info["updated_at"]
    file_stop = threading.Event()
    file_lock = threading.Lock()
    threads=[]
    for i in range(NUM_UPLOAD_THREADS):
        t = threading.Thread(target=csv_save, args=[data_q, "out_", file_stop, file_lock, port, updated_at])
        t.start()
        threads.append(t)
    print("Starting to read stdin...")
    stop.wait()
    print("Got stop signal from parent")
    data_q.join()
    file_stop.set()
    for t in threads:
        t.join()
    print("read {0} requests".format(n))

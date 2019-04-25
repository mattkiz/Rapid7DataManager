from gcp import storage
from collections import deque
import threading
import gzip
import shutil
import os

MaxEntries = 100000
filenum = 0
files_toupload = []
NUM_UPLOAD_THREADS = 3
name_lock = threading.Lock()


def upload_thread(filename, port, http_https):
    compressed_filename = '{0}.gz'.format(filename)
    print("compressing {0} to {1}".format(filename, compressed_filename))
    with open(filename, 'rb') as f_in:
        f_out = gzip.open(compressed_filename, 'wb+')
        shutil.copyfileobj(f_in, f_out)
    f_out.close()
    storage.save_file_to_cloud(compressed_filename, "raw_{1}_port_{0}/{2}".format(port, http_https, compressed_filename))
    os.remove(filename)
    os.remove(compressed_filename)


def csv_save(in_q, prefix, stop, lock:threading.Lock, port, http_https):
    global filenum
    name_lock.acquire()
    filename = "output/{0}{1}.csv".format(prefix, filenum)
    filenum += 1
    name_lock.release()
    f = open(filename, "w")
    i = 0
    threads = []
    while not stop.is_set():
        lock.acquire()
        if len(in_q) == 0:
            lock.release()
            continue
        item = in_q.popleft()
        lock.release()
        if i > MaxEntries:
            f.close()
            t = threading.Thread(target=upload_thread, args=[filename, port, http_https])
            t.start()
            threads.append(t)
            name_lock.acquire()
            filename = "output/{0}{1}.csv".format(prefix, filenum)
            filenum += 1
            name_lock.release()
            f = open(filename, "w")
            i = 0
        i+=1
        f.write(item)
    print("got stop... exiting other threads...")
    f.close()
    upload_thread(filename, port, http_https)
    for t in threads:
        t.join()


def main(data_q, stop, port, http_https):
    global filenum
    main_q = deque()
    file_stop = threading.Event()
    file_lock = threading.Lock()
    threads=[]
    for i in range(NUM_UPLOAD_THREADS):
        t = threading.Thread(target=csv_save, args=[main_q, "out_", file_stop, file_lock, port, http_https])
        t.start()
        threads.append(t)
    print("Starting to read stdin...")
    i = 0
    while not stop.is_set():
        if data_q.empty():
            continue
        main_q.append(data_q.get().decode("utf-8"))
        if i % 10000 == 0:
            print("upload has gotten {0}".format(i))
        i+=1
    while not data_q.empty():
        main_q.append(data_q.get().decode("utf-8"))
        if i % 10000 == 0:
            print("upload has gotten {0}".format(i))
        i+=1
    while len(main_q) > 0:
        continue
    file_stop.set()
    for t in threads:
        t.join()
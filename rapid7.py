import requests as rq
import os
import datetime
import time
import json

DEFAULT_CACHE_PATH="cache/sonar_urls.tmp"
STUDY_URL="https://us.api.insight.rapid7.com/opendata/studies/"
APIKEY="226a18ae-0dd8-489e-8da7-cf0014a17e85"


def read_cache(cache_path):
    if os.path.exists(cache_path):
        f = open(cache_path)
        urls_json = json.load(f)
        valid_urls = []
        for url in urls_json:
            timestamp = datetime.datetime.fromtimestamp(url["timestamp"])
            if timestamp + datetime.timedelta(days=1) < datetime.datetime.now():
                continue
            valid_urls.append(url)
        f.close()
        return valid_urls
    else:
        return []


def save_cache(cache_path, json_list):
    if os.path.exists(cache_path):
        os.remove(cache_path)
    f = open(cache_path, "w")
    json.dump(json_list, f)
    f.close()


def find_file_obj(json_list, filename):
    for url in json_list:
        if url["name"] == filename:
            return url
    return None


def find_in_study(filename, study_id):
    resp = rq.request("GET", STUDY_URL+study_id+"/", headers={"X-Api-Key": APIKEY})
    file_set = resp.json()["sonarfile_set"]
    return filename in file_set


def find_in_studies(filename):
    if find_in_study(filename, "sonar.http"):
        return "sonar.http"
    if find_in_study(filename, "sonar.https"):
        return "sonar.https"
    return None


def get_file_info(filename, study_id):
    resp = rq.request("GET", STUDY_URL+study_id+"/"+filename+"/", headers={"X-Api-Key": APIKEY})
    return resp.json()


def get_quota_info():
    resp = rq.request("GET", "https://us.api.insight.rapid7.com/opendata/quota/", headers={"X-Api-Key": APIKEY})
    return resp.json()


def get_download_url(filename, study_id):
    resp = rq.request("GET", STUDY_URL + study_id + "/" + filename + "/download/", headers={"X-Api-Key": APIKEY})
    return resp.json()["url"]


def check_quota(qinfo, filename):
    fc = input("Only {0} downloads left. Use one to get file {1}?".format(
        qinfo["quota_left"], filename))
    while fc not in "yYnN":
        print("Invalid response")
        fc = input("Only {0} downloads left. Use one to get file {1}?".format(
            qinfo["quota_left"], filename))
    if fc in "nN":
        print("Exiting...")
        exit()


def find_download_url(filename, force_create=False):
    study_id = find_in_studies(filename)
    if study_id is None:
        print("File is not found... Exiting")
        exit()
    file_info = get_file_info(filename, study_id)
    cache = read_cache(DEFAULT_CACHE_PATH)
    file_obj = find_file_obj(cache, filename)
    if file_obj is None:
        qinfo = get_quota_info()
        if not force_create:
            check_quota(qinfo, filename)
        cache_entry = file_info
        cache_entry["timestamp"] = time.time()
        cache_entry["url"] = get_download_url(filename, study_id)
        cache.append(cache_entry)
    file_obj = find_file_obj(cache, filename)
    save_cache(DEFAULT_CACHE_PATH, cache)
    return file_obj
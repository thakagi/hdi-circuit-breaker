# -*- coding: utf-8 -*-

import re
import requests
import urllib3
urllib3.disable_warnings()
import json
import datetime
from optparse import OptionParser

JOB_STATE = "RUNNING"
APPS_API = "/ws/v1/cluster/apps"
USER_NAMES = ["sshuser"]

def filter_by_user(user):
    return True if (user in USER_NAMES) else False

def filter_by_state(state):
    return True if (state == JOB_STATE) else False

def filter_by_application_type(app_type):
    return True if (app_type == options.application) else False

def check_long_running_job(elapsed_time):
    return True if (elapsed_time > options.threshold) else False

def terminate_job(app_id, app_url):
    payload = {"state" : "KILLED"}
    headers = {'content-type': 'application/json'}
    print("Terminating : " + app_url + "/" + app_id)
    r = requests.put(app_url + "/" + app_id + "/state", data=json.dumps(payload), headers=headers, verify=False) #{"state": "KILLED"}
    print(r.text)


def run():
    baseurl = "http://" + options.rm_ip + ":8088"
    app_url = baseurl + APPS_API
    print(app_url)
    r = requests.get(app_url, verify=False)
    res = json.loads(r.text)
    for i in (res['apps']['app']):
        # 1. Filter by user
        if filter_by_user(i['user']):
            # 2. Filter by state (RUNNING)
            if filter_by_state(i['state']):
                # 3. Filter by application type
                if filter_by_application_type(i['applicationType']):
                    # 4. Check if long-running job
                    if check_long_running_job(i['elapsedTime']):
                        terminate_job(i['id'], app_url)

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-i', '--ipaddress', dest='rm_ip', help='RM IP Address', default="10.0.0.14"), # Resource Manager IP Address
    parser.add_option('-t', '--threshold', dest='threshold', type=int, help='threshold', default="10000"), # Epoch millisecond
    parser.add_option('-a', '--application', dest='application', help='application', default="MAPREDUCE"), # MAPREDUCE, SPARK, HIVE, etc
    (options, args) = parser.parse_args()
    run()


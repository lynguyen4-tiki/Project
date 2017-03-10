from urllib import parse
import requests
import json
import datetime

TIME_START = datetime.datetime.strptime('2017-03-03 03:50:00', '%Y-%m-%d %H:%M:%S')
NAME_NODE = 'openhab-dtvsr'
URL = 'http://188.166.238.158:32485/query'
payload = {}
payload['db'] = 'k8s'

MAX_TIME = 350
QUERY_CPU = 'cpu/usage_rate'
QUERY_RAM = 'memory/usage'
QUERY_DISK = 'filesystem/usage'
ONE_MINUTE = datetime.timedelta(minutes=1)

# a = datetime.datetime(2017,3,3,3,3,0,0)

def createQuery(_pod_name, measurement, timeMin, timeMax):
    return 'SELECT "value" FROM "'+ measurement +'" WHERE "type" = \'pod_container\' AND "namespace_name" = \'kube-system\' AND "pod_name" = \''+ _pod_name+ '\' AND "time">=\''+ str(timeMin)+'\' AND "time"<\'' + str(timeMax)+'\''

def getData(timeStart):
    queryCPU = createQuery(NAME_NODE, QUERY_CPU, timeStart, timeStart + ONE_MINUTE)
    queryMEM = createQuery(NAME_NODE, QUERY_DISK, timeStart, timeStart + ONE_MINUTE)
    queryRAM = createQuery(NAME_NODE, QUERY_RAM, timeStart, timeStart + ONE_MINUTE)

    payload['q'] = queryCPU + ';' + queryMEM + ';' + queryRAM
    data = parse.urlencode(payload)

    mea = requests.get(URL, data)
    if (mea.ok):
        d = json.loads(mea.content.decode())
        vCPU = int(d['results'][0]['series'][0]['values'][0][1])
        vMEM = int(d['results'][1]['series'][0]['values'][0][1] / 1024 / 1024)
        vRAM = int(d['results'][2]['series'][0]['values'][0][1] / 1024 / 1024)
        return [vCPU, vMEM, vRAM]

    return [0,0,0]
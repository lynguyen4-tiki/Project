import argparse

def createParse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-DEBUG', dest='DEBUG', type = bool, default= False)
    parser.add_argument('-MODE_EPS', dest='MODE_EPS', type = int, default= 0)
    parser.add_argument('-eps', dest='eps', type = int, default= 0)
    parser.add_argument('-ext', dest='ext', type=str, default='')
    parser.add_argument('-DELTA_K', dest='DELTA_K', type = int, default=3)
    parser.add_argument('-k', dest='k', type=int, default= 3 + 4)
    parser.add_argument('-h1', dest='h1', type=int, default=1)
    parser.add_argument('-h2', dest='h2', type=int, default=1)
    parser.add_argument('-h3', dest='h3', type=int, default=1)
    parser.add_argument('-band', dest='band', type=int, default=50)
    parser.add_argument('-IP_SERVER', dest = 'IP_SERVER', type=str, default='localhost')
    parser.add_argument('-PORT_NODE', dest='PORT_NODE', type=int, default=9407)
    parser.add_argument('-PORT_USER', dest='PORT_USER', type=int, default=7021)
    parser.add_argument('-MAX_NUMBER_NODE', dest='MAX_NUMBER_NODE', type=int, default=50)
    parser.add_argument('-DELTA_EPS', dest='DELTA_EPS', type=int, default=1)
    parser.add_argument('-NUM_MONITOR', dest='NUM_MONITOR', type=int, default=120)
    parser.add_argument('-TIME_CAL_NETWORK', dest='TIME_CAL_NETWORK', type=float, default=3.0)
    return parser

def readConfig(fName:str):
    data = ''
    try:
        with open(fName, 'r') as f:
            while 1:
                temp = f.readline().replace('\n', '')
                if (temp == ''):
                    break
                data += temp + ' '
    except Exception as e:
        return None

    data = data.rstrip()
    if (len(data) == 0):
        return None
    data = data.split(' ')
    arg = createParse()
    return arg.parse_args(data)
import argparse

def createParse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-DEBUG', dest='DEBUG', type=bool, default=False)
    parser.add_argument('-DATA_MODE', dest='DATA_MODE', type = int, default= 5)
    parser.add_argument('-IP_SERVER', dest='IP_SERVER', type=str, default='localhost')
    parser.add_argument('-PORT_NODE', dest='PORT_NODE', type=int, default=9407)
    parser.add_argument('-DELTA_TIME', dest='DELTA_TIME', type=float, default=2.0)
    parser.add_argument('-SAMPLE_ON_CIRCLE', dest='SAMPLE_ON_CIRCLE', type=int, default=10)
    parser.add_argument('-NAME', dest='NAME', type=str, default='Mon_')
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
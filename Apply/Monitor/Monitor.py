import time
import threading
import socket
try:
    import Common.MyEnum as MyEnum
    import Common.MyParser as MyParser
    import Common.GetDataFromServer as GetData
    import Apply.Monitor.ParseMon as ParseMon
except ImportError:
    import MyEnum
    import MyParser
    import GetDataFromServer as GetData
    import ParseMon

import sys
import os

DEBUG = False
DATA_MODE = MyEnum.MonNode.DATA_GEN_AUTO.value

IP_SERVER  = 'localhost'
PORT_NODE = 9407
DELTA_TIME = 2.0
SAMPLE_ON_CIRCLE = 10
bStartMon = False
bStop = False
session = -1

################################################################################
#read from file config to get information
def readConfig():
    global myName, startTime, addName, fileData, DEBUG, DATA_MODE, IP_SERVER, PORT_NODE, DELTA_TIME, SAMPLE_ON_CIRCLE
    addName = ''
    fName = 'config/monConfig'
    try:
        addName = sys.argv[1]
    except Exception:
        pass
    myName = addName

    fName += str(addName) + '.cfg'

    arg = ParseMon.readConfig(fName)
    if (arg == None):
        return
    myName = arg.NAME
    DEBUG = arg.DEBUG
    DATA_MODE = arg.DATA_MODE
    IP_SERVER = arg.IP_SERVER
    PORT_NODE = arg.PORT_NODE
    DELTA_TIME = arg.DELTA_TIME
    SAMPLE_ON_CIRCLE = arg.SAMPLE_ON_CIRCLE

#create message to send to server
def createMessage(strRoot = '', arg = {}):
    strResult = str(strRoot)
    for k, v in arg.items():
        strResult = strResult + ' ' + str(k) + ' ' + str(v)

    return strResult

#server send data to update argument
def updateArg(arg, sock: socket.socket):
    global h1, h2, h3, eps, bound, session
    global bStartMon, V

    if (arg.h1 != None):
        h1 = arg.h1[0]

    if (arg.h2 != None):
        h2 = arg.h2[0]

    if (arg.h3 != None):
        h3 = arg.h3[0]

    if (arg.session != None):
        session = arg.session[0]

    if (arg.bound != None):
        bound = arg.bound[0]

    if (arg.eps != None):
        eps = arg.eps[0]

    #server updates new coefficient so we have to send new value to server
    if (arg.session != None ):
        bStartMon = False
        time.sleep(DELTA_TIME + 1)
        V = h1 * dtCPU + h2 * dtRAM + h3 * dtMEM
        bStartMon = True
        sendCurrentvalue(sock, bound)
        return

# send current value to server
def sendCurrentvalue(sock:socket.socket, bound:int):
    global V
    global eps

    V = h1 * dtCPU + h2 * dtRAM + h3 * dtMEM

    if (V > bound):
        dataSend = createMessage('', {'-type': MyEnum.MonNode.NODE_SET_DATA.value})
        dataSend = createMessage(dataSend, {'-ses': session})
        dataSend = createMessage(dataSend, {'-value': V})
        sock.sendall(bytes(dataSend.encode()))
        os.system('clear')
        print(myName + ': ' + str(V) + '___%d' %(eps))

################################################################################
#communication with server
def workWithServer(sock : socket.socket):
    global V, dtCPU, dtRAM, dtMEM, bStop

    readConfig()

    try:
        # send name
        dataSend = createMessage('', {'-type': MyEnum.MonNode.NODE_SET_NAME.value})
        dataSend = createMessage(dataSend, {'-name': myName})
        sock.sendall(bytes(dataSend.encode('utf-8')))

        #listen command from server
        while 1:
            try:
                dataRecv = sock.recv(1024).decode()
                if (dataRecv == ''):
                    return
                arg = parser.parse_args(dataRecv.lstrip().split(' '))
                type = arg.type[0]
            except socket.error:
                return
            except Exception:
                continue
            #server update argument
            if type == MyEnum.MonNode.SERVER_SET_ARG.value:
                updateArg(arg, sock)
                continue
            #server need data from this node
            if type == MyEnum.MonNode.SERVER_GET_DATA.value:
                bound = arg.bound[0]
                sendCurrentvalue(sock, bound)
                continue
    except socket.error:
        pass

    finally:
        bStop = True
        sock.close()

def getData():

    if (DATA_MODE == MyEnum.MonNode.DATA_FROM_INFLUXDB.value):
        global timeStart
        if (timeStart > GetData.ONE_MINUTE * GetData.MAX_TIME + startTime):
            timeStart = startTime
        result = GetData.getData(timeStart)
        timeStart += GetData.ONE_MINUTE
        return result

    elif (DATA_MODE == MyEnum.MonNode.DATA_GEN_AUTO.value):
        global fileData
        line = fileData.readline().replace('\n','')
        if (line == ''):
            fileData.close()
            fileData = open('data/data' + str(addName) + '.dat', 'r')
            line = fileData.readline().replace('\n', '')

        strData = line.split(' ')
        iData = [0,0,0]
        iData[0] = int(strData[0])
        iData[1] = int(strData[1])
        iData[2] = int(strData[2])

        return iData

#monitor data
def monData(sock: socket.socket):

    global V, dtCPU, dtRAM, dtMEM, addName
    V = 0
    dtCPU = 0
    dtRAM = 0
    dtMEM = 0

    if (DATA_MODE == MyEnum.MonNode.DATA_FROM_INFLUXDB.value):
        global timeStart, startTime
        value = 0
        if (len(addName) == 0):
            value = 0
        else:
            value = int(addName)
        startTime = GetData.TIME_START + GetData.ONE_MINUTE * GetData.MAX_TIME * value
        timeStart = startTime
    elif (DATA_MODE == MyEnum.MonNode.DATA_GEN_AUTO.value):
        global  fileData
        fileData = open('data/data' + str(addName) + '.dat', 'r')

    while (not bStop):

        tmpCPU, tmpRAM, tmpMEM = getData()

        if (bStartMon == False):
            dtCPU = tmpCPU
            dtRAM = tmpRAM
            dtMEM = tmpMEM
        else:
            tmpV = h1 * tmpCPU + h2 * tmpRAM + h3 * tmpMEM
            if (abs(tmpV - V) > eps):
                dtCPU = tmpCPU
                dtRAM = tmpRAM
                dtMEM = tmpMEM

                try:
                    sendCurrentvalue(sock, -1)
                except socket.error:
                    return

        time.sleep(DELTA_TIME)

    if (DATA_MODE == MyEnum.MonNode.DATA_GEN_AUTO.value):
        fileData.close()
    elif (DATA_MODE == MyEnum.MonNode.DATA_FROM_INFLUXDB.value):
        pass

################################################################################
################################################################################
#init connection
readConfig()
#monData(None)
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (IP_SERVER, PORT_NODE)
    sock.connect(server_address)

    #init parser
    parser = MyParser.createParser()

    #init thread
    thMon = threading.Thread(target=monData, args=(sock,))
    thWork = threading.Thread(target=workWithServer, args=(sock,))

    thMon.start()
    thWork.start()

    #wait for all thread running
    thWork.join()
    thMon.join()

except socket.error as e:
    print(e)
    pass
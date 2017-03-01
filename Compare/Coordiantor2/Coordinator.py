import json
import threading
import socket
import Common.MyEnum as MyEnum
import Common.MyParser as MyParser
import time
import os
import random

DEBUG = True

# init arguments
k = 2       # number elements in top
h1 = 1      # Coefficient of the 1st element in integrative function
h2 = 1      # Coefficient of the 2nd element in integrative function
h3 = 1      # Coefficient of the 3rd element in integrative function
session = 0 # the number of session
eps = 3     # epsilon
band = 10   # limit bandwidth

currentBand = 0
netIn  = 0
netOut = 0

topK = []
nameTop = []
sockTop = []
valueKP1 = 0

lstSock = []
lstName = []

bUserConnect = False

IP_SERVER  = 'localhost'
PORT_NODE = 7049
PORT_USER = 7012
MAX_NUMBER_NODE = 50
DELTA_BAND = 5
DELTA_EPS = 2

TIME_CAL_NETWORK = 2.0

################################################################################
def addNetworkIn(value:int):
    global netIn
    global lockNetIn
    lockNetIn.acquire()
    netIn += value
    lockNetIn.release()

def addNetworkOut(value:int):
    global netOut
    global lockNetOut
    lockNetOut.acquire()
    netOut += value
    lockNetOut.release()

def monNetwork():
    global lockNetIn
    global lockNetOut
    global netIn
    global netOut

    while 1:
        time.sleep(TIME_CAL_NETWORK)
        lockNetIn.acquire()
        nIn = netIn / TIME_CAL_NETWORK
        netIn = 0
        lockNetIn.release()

        lockNetOut.acquire()
        nOut = netOut / TIME_CAL_NETWORK
        netOut = 0
        lockNetOut.release()

        # if DEBUG:
        #     print('netIn = %.2f _________ netOut = %.2f' %(nIn, nOut) )

################################################################################
def swap(i:int, j:int):
    tmp = topK[i]
    topK[i] = topK[j]
    topK[j] = tmp

    tmp = nameTop[i]
    nameTop[i] = nameTop[j]
    nameTop[j] = tmp

    tmp = sockTop[i]
    sockTop[i] = sockTop[j]
    sockTop[j] = tmp

def createMessage(strRoot = '', arg = {}):
    strResult = str(strRoot)
    for k, v in arg.items():
        strResult = strResult + ' ' + str(k) + ' ' + str(v)

    return strResult

#return index of node in topK list if the node is in the list, else, return -1
def findNodeInTop(strname : str):
    global lockTop
    iRet = -1
    lockTop.acquire()
    for i in range(len(topK)):
        if (nameTop[i] == strname):
            iRet = i
            break
    lockTop.release()
    return iRet

def forceGetData(bound:int):
    global lockLst
    data = createMessage('', {'-type':MyEnum.MonNode.SERVER_GET_DATA.value})
    data = createMessage(data, {'-bound':bound})
    for s in lstSock:
        try:
            s.sendall(bytes(data.encode()))
            addNetworkOut(len(data))
        except socket.error:
            pass

def init():
    global serverForNode, serverForUser
    global lockCount, lockLst, lockTop, lockNetIn, lockNetOut, lockUpdate
    global parser

    for i in range(k):
        topK.append(0)
        sockTop.append(None)
        nameTop.append("")

    #init server to listen monitor node
    serverForNode = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverForNode.bind((IP_SERVER, PORT_NODE))
    serverForNode.listen(MAX_NUMBER_NODE)

    #init server to listen user node
    serverForUser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverForUser.bind((IP_SERVER, PORT_USER))
    serverForUser.listen(1)

    #init synchronize variable
    lockCount = threading.Lock()
    lockLst = threading.Lock()
    lockTop = threading.Lock()
    lockNetIn = threading.Lock()
    lockNetOut = threading.Lock()
    lockUpdate = threading.Lock()

    #init argument parser
    parser = MyParser.createParser()

def printTop():
    global userSock
    data = json.dumps([topK, nameTop])
    if (DEBUG):
        print(data)

    try:
        userSock.sendall(data.encode())
    except Exception:
        return

################################################################################
def sendOneSock(low:float, high:float, sock:socket.socket):
    dataSend = ''

    dataSend = createMessage(dataSend, {'-low':low})
    dataSend = createMessage(dataSend, {'-high':high})
    dataSend = createMessage(dataSend, {'-type': MyEnum.MonNode.SERVER_SET_ARG.value})
    try:
        sock.sendall(bytes(dataSend.encode()))
        addNetworkOut(len(dataSend))
    except socket.error:
        pass

def sendAllSock(high:float):
    dataSend = ''
    dataSend = createMessage(dataSend, {'-high':high})
    dataSend = createMessage(dataSend, {'-low': -1})
    dataSend = createMessage(dataSend, {'-type':MyEnum.MonNode.SERVER_SET_ARG.value})
    for s in lstSock:
        try:
            s.sendall(bytes(dataSend.encode()))
            addNetworkOut(len(dataSend))
        except socket.error:
            pass

def sendBoundTo(pos : int):
    if (pos < 0):
        return

    if (pos == k):
        if (valueKP1 == 0):
            return
        highBound = (valueKP1 + topK[k-1]) / 2.0
        sendAllSock(highBound)
        return

    if (topK[pos] == 0):
        return

    if (pos == 0):
        highBound = -1
    else:
        highBound = (topK[pos] + topK[pos - 1]) / 2.0

    if (pos == k - 1):
        if (valueKP1 == 0):
            lowBound = -1
        else:
            lowBound = (topK[pos] + valueKP1) / 2.0
    else:
        if (topK[pos + 1] == 0):
            lowBound = -1
        else:
            lowBound = (topK[pos] + topK[pos + 1]) / 2.0

    sendOneSock(lowBound, highBound, sockTop[pos])

def sendBoundAround(pos: int):
    sendBoundTo(pos - 1)
    sendBoundTo(pos)
    sendBoundTo(pos + 1)

#add new element in top
def addToTopK(value: int, name: str, sock : socket.socket):
    global lockTop, lockLst, valueKP1
    d = 0
    c = k - 1
    g = int((d + c) /2)

    lockTop.acquire()
    while (d <= c):
        if (topK[g] > value):
            d = g + 1
        elif (topK[g] < value):
                c = g - 1
        else:
            break
        g = int((d + c) / 2)

    g = d

    lockLst.acquire()
    lstSock.remove(sock)
    lstName.remove(name)
    if (nameTop[k-1] != ''):
        lstSock.append(sockTop[k-1])
        lstName.append(nameTop[k-1])
        valueKP1 = topK[k-1]
    lockLst.release()

    for i in range(k-1, g, -1):
        topK[i] = topK[i-1]
        nameTop[i] = nameTop[i-1]
        sockTop[i] = sockTop[i-1]

    topK[g] = value
    nameTop[g] = name
    sockTop[g] = sock
    lockTop.release()
    printTop()
    #update filter
    sendBoundAround(g)
    if (g != k-1 and valueKP1 != 0):
        sendBoundTo(k)

#change the order of the element in top
def changeOrderInTop(value : int, iNodeInTop: int) :
    global lockTop, valueKP1, lockLst
    tmpIndex = iNodeInTop
    oldBound = (topK[k-1] + valueKP1) / 2.0
    if (value > topK[iNodeInTop]):
        # pull up
        lockTop.acquire()
        topK[iNodeInTop] = value
        while (iNodeInTop > 0 and value > topK[iNodeInTop - 1]):
            iNodeInTop -= 1
            swap(iNodeInTop, iNodeInTop + 1)
        #update filter
        sendBoundAround(iNodeInTop)
        if (tmpIndex != iNodeInTop):
            sendBoundTo(tmpIndex + 1)
            sendBoundTo(tmpIndex)
        lockTop.release()
        printTop()
        return

    if (value < topK[iNodeInTop]):
        #pull down
        lockTop.acquire()
        topK[iNodeInTop] = value
        while (iNodeInTop < k -1 and value < topK[iNodeInTop + 1]):
            iNodeInTop += 1
            swap(iNodeInTop, iNodeInTop - 1)
        lockTop.release()
        # call all node to get lastest data if the value k-th element decreases
        if (iNodeInTop == k - 1):
            if (value > oldBound):
                sendBoundAround(k - 1)
                if (tmpIndex != iNodeInTop):
                    sendBoundTo(tmpIndex)
                    sendBoundTo(tmpIndex - 1)
            else:
                valueKP1 = topK[k-1] / 2
                if (valueKP1 == 0):
                    valueKP1 = 1
                if (tmpIndex != iNodeInTop):
                    sendBoundTo(tmpIndex)
                    sendBoundTo(tmpIndex - 1)
                sendBoundAround(k - 1)
        else:
            sendBoundAround(iNodeInTop)
            if (tmpIndex != iNodeInTop):
                sendBoundTo(tmpIndex)
                sendBoundTo(tmpIndex - 1)
        printTop()
        return

def updateTopK(value:int, name : str, s:socket.socket):
    global valueKP1, lockUpdate
    #lockUpdate.acquire()
    nameNode = name
    iNodeInTop = findNodeInTop(nameNode)
    # this change doesn't effect to top
    if (iNodeInTop == -1 and value < topK[k-1]):
        if (value > valueKP1):
            valueKP1 = value
            sendBoundTo(k-1)
            sendBoundTo(k)
        return

    #an element goes in Top
    if (iNodeInTop == -1 and value > topK[k - 1]):
        addToTopK(value, name, s)
        return

    changeOrderInTop(value, iNodeInTop)
    #lockUpdate.release()

#remove the node that is disconnected
def removeInTop(strName:str, s:socket.socket):
    global lockTop, valueKP1
    iIndex = findNodeInTop(strName)
    if (iIndex == -1):
        lockLst.acquire()
        lstSock.remove(s)
        lstName.remove(strName)
        lockLst.release()
        return
    lockTop.acquire()
    for i in range(iIndex, k-1):
        swap(i, i+ 1)
    topK[k-1] = 0
    nameTop[k-1] = ''
    sockTop[k-1] = None
    valueKP1 = 0
    lockTop.release()
    forceGetData(0)
    printTop()
    pass

def updateArg(arg):
    global h1, h2, h3, band, k

    if (arg.h1 != None):
        h1 = arg.h1[0]

    if (arg.h2 != None):
        h2 = arg.h2[0]

    if (arg.h3 != None):
        h3 = arg.h3[0]

    if (arg.band != None):
        band = arg.band[0]

    if (arg.k != None):
        k = arg.k[0]

################################################################################
def workWithNode(s : socket.socket, address):
    global countNode
    global lockCount
    global lockLst

    try:
        #receive name
        dataRecv = s.recv(1024).decode()
        addNetworkIn(len(dataRecv))
        try:
            if (dataRecv != ''):
                arg = parser.parse_args(dataRecv.lstrip().split(' '))
                nameNode = arg.name[0]
                nameNode = str(address) + nameNode
        except socket.error:
            return
        except Exception:
            pass

        lockLst.acquire()
        lstSock.append(s)
        lstName.append(nameNode)
        lockLst.release()

        #send coefficient, lower bound, epsilon
        dataSend = createMessage('', {'-h1':h1})
        dataSend = createMessage(dataSend, {'-h2': h2})
        dataSend = createMessage(dataSend, {'-h3': h3})
        dataSend = createMessage(dataSend, {'-bound': valueKP1})
        dataSend = createMessage(dataSend, {'-ses': session})
        dataSend = createMessage(dataSend, {'-type': MyEnum.MonNode.SERVER_SET_ARG.value})
        s.sendall(bytes(dataSend.encode('utf-8')))
        addNetworkOut(len(dataSend))

        #receive current value
        while 1:
            try:
                dataRecv = s.recv(1024).decode()
                addNetworkIn(len(dataRecv))
                if (dataRecv != ''):
                    arg = parser.parse_args(dataRecv.lstrip().split(' '))
                    nodeSession = arg.session[0]
                    nodeValue = arg.value[0]
                    if (nodeSession == session):
                        updateTopK(nodeValue, nameNode, s)
                else:
                    return

            except socket.error:
                return
            except Exception:
                continue

    except socket.error:
        pass

    finally:
        s.close()
        removeInTop(nameNode, s)

        lockCount.acquire()
        countNode -= 1
        lockCount.release()

def acceptNode(server):
    global countNode
    global lockCount
    countNode = 0
    while (1):
        print('%d\n' %(countNode))
        if (countNode >= MAX_NUMBER_NODE):
            time.sleep(1)
            continue

        (nodeSock, addNode) = server.accept()

        lockCount.acquire()
        countNode += 1
        lockCount.release()

        threading.Thread(target=workWithNode, args=(nodeSock, addNode,)).start()
################################################################################
def acceptUser(server : socket.socket):
    global  userSock
    while (1):
        (userSock, addressUser) = server.accept()
        workWithUser(userSock)

def workWithUser(s : socket.socket):
    global parser
    global bUserConnect
    bUserConnect = True
    try:
        while 1:
            dataRecv = s.recv(1024).decode()
            if (dataRecv == ''):
                return
            arg = parser.parse_args(dataRecv.lstrip().split(' '))
            type = arg.type[0]
            if (type == MyEnum.User.USER_SET_ARG.value):
                updateArg(arg)
    except socket.error:
        return
    finally:
        bUserConnect = False
        s.close()

################################################################################
################################################################################
init()

# create thread for each server
thNode = threading.Thread(target=acceptNode, args=(serverForNode,))
thNode.start()

thMon = threading.Thread(target=monNetwork, args=())
thMon.start()


thUser = threading.Thread(target=acceptUser, args=(serverForUser,))
thUser.start()

#wait for all thread terminate
thNode.join()
thMon.join()

thUser.join()
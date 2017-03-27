import json
import threading
import socket
import time
try:
    import Common.MyEnum as MyEnum
    import Common.MyParser as MyParser
    import Compare.Coordiantor2.ParseCor2 as ParseCor2
except ImportError:
    import MyEnum
    import MyParser
    import ParseCor2

DEBUG = True
serverForNode = socket.socket()
serverForUser = socket.socket()

ext = ''

# init arguments
DELTA_K = 3
k = 4 + DELTA_K      # number elements in top
h1 = 1      # Coefficient of the 1st element in integrative function
h2 = 1      # Coefficient of the 2nd element in integrative function
h3 = 1      # Coefficient of the 3rd element in integrative function
session = 0 # the number of session

currentK = 0

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
PORT_NODE = 9407
PORT_USER = 7021
MAX_NUMBER_NODE = 50
FILE_MON_NET = 'data/NetWorkLoad_'+ ext+'.dat'
FILE_MON_TOP = 'data/Top_' + ext + '.dat'

NUM_MONITOR = 120
TIME_CAL_NETWORK = 3.0

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

def saveNetWorkLoad(netWorkLoad : int):
    with open(FILE_MON_NET, 'a') as f:
        f.write(str(netWorkLoad) + '\n')

def monNetwork():
    global lockNetIn
    global lockNetOut
    global netIn
    global netOut
    global countNode
    countTime = 0

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

        if DEBUG:
            print('netIn = %.2f _________ netOut = %.2f' %(nIn, nOut) )
        if (countNode > 0):
            countTime += 1
            print('CountTime = %d' %(countTime))
            if (countTime <= NUM_MONITOR):
                saveNetWorkLoad(int(nIn + nOut))

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

def sendDataToAll(data:str):
    global lockLst
    lockLst.acquire()
    for s in lstSock:
        try:
            s.sendall(bytes(data.encode()))
            addNetworkOut(len(data))
        except Exception:
            pass
    lockLst.release()

def forceGetData(bound:int):
    global lockLst
    data = createMessage('', {'-type':MyEnum.MonNode.SERVER_GET_DATA.value})
    data = createMessage(data, {'-bound':bound})
    sendDataToAll(data)

def appendToTop(value = 0, sock = None, name = ''):
    global topK, sockTop, nameTop
    topK.append(value)
    sockTop.append(sock)
    nameTop.append(name)

def readConfig(fName=''):
    global DEBUG, DELTA_K, k, h1, h2, h3, IP_SERVER, PORT_NODE, FILE_MON_TOP
    global PORT_USER, MAX_NUMBER_NODE, FILE_MON_NET, NUM_MONITOR, TIME_CAL_NETWORK

    arg = ParseCor2.readConfig(fName)
    if (arg == None):
        return

    DEBUG = arg.DEBUG
    ext = arg.ext
    DELTA_K = arg.DELTA_K
    k = arg.k + DELTA_K
    h1 = arg.h1
    h2 = arg.h2
    h3 = arg.h3
    IP_SERVER = arg.IP_SERVER
    MAX_NUMBER_NODE = arg.MAX_NUMBER_NODE
    FILE_MON_NET = 'data/NetWorkLoad_' + ext + '.dat'
    FILE_MON_TOP = 'data/Top_' + ext + '.dat'
    NUM_MONITOR = arg.NUM_MONITOR
    TIME_CAL_NETWORK = arg.TIME_CAL_NETWORK

def init():
    global serverForNode, serverForUser
    global lockCount, lockLst, lockTop, lockNetIn, lockNetOut, lockUpdate
    global parser

    try:
        readConfig('config/corConfig.cfg')
    except Exception:
        pass

    for i in range(k):
        appendToTop()

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

    #delete old file
    f = open(FILE_MON_NET, 'w')
    f.close()

    #init argument parser
    parser = MyParser.createParser()

def printTop():
    global userSock, lockTop
    tmpTop = []
    tmpName = []

    lockTop.acquire()
    for i in range(k - DELTA_K):
        if (tmpName == ''):
            break
        tmpTop.append(topK[i])
        tmpName.append(nameTop[i])
    lockTop.release()

    data = json.dumps([tmpTop, tmpName])
    # if (DEBUG):
    #     print(data)

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

def sendBoundTo(pos : int):
    global currentK, valueKP1
    if (pos < 0):
        return

    if (pos >= currentK - 1):
        return

    if (topK[pos] == 0):
        return

    if (pos == 0):
        highBound = -1
    else:
        highBound = (topK[pos] + topK[pos - 1]) / 2.0

    if (pos == currentK - 1):
        lowBound = valueKP1
    else:
         lowBound = (topK[pos] + topK[pos + 1]) / 2.0

    sendOneSock(lowBound, highBound, sockTop[pos])

def sendBoundAround(pos: int):
    sendBoundTo(pos - 1)
    sendBoundTo(pos)
    sendBoundTo(pos + 1)

def sendBoundSwap(pos1:int, pos2:int):
    sendBoundAround(pos1)

#add new element in top
def addToTopK(value: int, name: str, sock : socket.socket):
    global lockTop, lockLst, valueKP1, currentK, k
    d = 0
    c = currentK - 1
    g = int((d + c) /2)
    tmpSock = None

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
    if (nameTop[currentK-1] != ''):
        lstSock.append(sockTop[currentK-1])
        lstName.append(nameTop[currentK-1])
        tmpSock = sockTop[currentK-1]
        valueKP1 = topK[currentK-1]
    lockLst.release()

    for i in range(currentK-1, g, -1):
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
    if (tmpSock != None):
        sendOneSock(-1, valueKP1, tmpSock)

#change the order of the element in top
def changeOrderInTop(value : int, iNodeInTop: int) :
    global lockTop, valueKP1, lockLst, currentK
    tmpIndex = iNodeInTop

    if (value > topK[iNodeInTop]):
        # pull up
        lockTop.acquire()
        topK[iNodeInTop] = value
        while (iNodeInTop > 0 and value > topK[iNodeInTop - 1]):
            iNodeInTop -= 1
            swap(iNodeInTop, iNodeInTop + 1)
        #update filter
        sendBoundSwap(iNodeInTop, tmpIndex)
        lockTop.release()
        printTop()
        return

    if (value < topK[iNodeInTop]):
        #pull down
        lockTop.acquire()
        topK[iNodeInTop] = value
        while (iNodeInTop < currentK -1 and value < topK[iNodeInTop + 1]):
            iNodeInTop += 1
            swap(iNodeInTop, iNodeInTop - 1)
        lockTop.release()
        # process if the value k-th element decreases
        if (iNodeInTop == currentK - 1):

            if (value >= valueKP1):
                sendBoundSwap(iNodeInTop, tmpIndex - 1)
            else:
                lockTop.acquire()
                lstSock.append(sockTop[iNodeInTop])
                lstName.append(nameTop[iNodeInTop])
                nameTop[iNodeInTop] = ''
                tmpSock = sockTop[iNodeInTop]
                sockTop[iNodeInTop] = None
                topK[iNodeInTop] = 0
                currentK -= 1
                sendOneSock(-1, valueKP1, tmpSock)
                sendBoundSwap(iNodeInTop, tmpIndex - 1)
                lockTop.release()

                if (currentK < k - DELTA_K):
                    valueKP1 = 0
                    forceGetData(0)

        else:
            sendBoundSwap(iNodeInTop, tmpIndex - 1)
        printTop()
        return

def updateTopK(value:int, name : str, s:socket.socket):
    global valueKP1,currentK

    nameNode = name
    iNodeInTop = findNodeInTop(nameNode)

    if (iNodeInTop == -1):
        # this change doesn't effect to top
        if (value <= valueKP1):
            sendOneSock(-1, valueKP1, s)
            return

        # an element goes in Top
        if (currentK < k):
            currentK += 1
            addToTopK(value, name, s)
            return

        #currentK == k, doesn't effect to top, but effect filters
        if (value < topK[k-1]):
            valueKP1 = value
            sendBoundTo(k-1)
            sendOneSock(-1, valueKP1, s)
            return

        #replace an element in top
        addToTopK(value, name, s)
        return

    changeOrderInTop(value, iNodeInTop)

#remove the node that is disconnected
def removeInTop(strName:str, s:socket.socket):
    global lockTop, valueKP1, currentK
    iIndex = findNodeInTop(strName)
    if (iIndex == -1):
        lockLst.acquire()
        lstSock.remove(s)
        lstName.remove(strName)
        lockLst.release()
        return

    lockTop.acquire()
    for i in range(iIndex, currentK-1):
        swap(i, i+ 1)
    topK[currentK-1] = 0
    nameTop[currentK-1] = ''
    sockTop[currentK-1] = None
    currentK -= 1
    lockTop.release()
    if (currentK < k - DELTA_K):
        valueKP1 = 0
        forceGetData(0)
    printTop()
    pass

def updateArg(arg):
    global h1, h2, h3, k, lockTop, valueKP1, session, currentK
    dataSend = ''

    if (arg.h1 != None):
        h1 = arg.h1[0]
        dataSend = createMessage(dataSend, {'-h1' : h1})

    if (arg.h2 != None):
        h2 = arg.h2[0]
        dataSend = createMessage(dataSend, {'-h2': h2})

    if (arg.h3 != None):
        h3 = arg.h3[0]
        dataSend = createMessage(dataSend, {'-h3': h3})

    newK = k
    if (arg.k != None):
        newK = arg.k[0]
        newK += DELTA_K

    #update new coefficent
    if (dataSend != ''):
        lockTop.acquire()
        for i in range(k):
            lstName.append(nameTop.pop(0))
            lstSock.append(sockTop.pop(0))
            topK.pop(0)
        valueKP1 = 0
        k = newK
        for i in range(k):
            appendToTop()
        lockTop.release()
        session += 1
        dataSend = createMessage(dataSend, {'-ses': session})
        dataSend = createMessage(dataSend, {'-type': MyEnum.MonNode.SERVER_SET_ARG.value})
        sendDataToAll(dataSend)
    else:
        if (newK < k):
            lockTop.acquire()
            valueKP1 = topK[newK]
            tmpSock = sockTop[newK]
            for i in range(k - newK):
                topK.pop(newK)
                lstName.append(nameTop.pop(newK))
                lstSock.append(sockTop.pop(newK))
            lockTop.release()
            k = newK
            if (currentK > newK):
                currentK = newK
            sendBoundTo(k - 1)
            sendOneSock(-1, valueKP1, tmpSock)
        elif (newK > k):
            lockTop.acquire()
            valueKP1 = 0
            for i in range(newK - k):
                appendToTop()
            lockTop.release()
            k = newK
            if (currentK < newK - DELTA_K):
                forceGetData(0)

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
        printTop()
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

try:
    #wait for all thread terminate
    thNode.join()
    thMon.join()
    thUser.join()
except KeyboardInterrupt:
    serverForNode.close()
    serverForUser.close()
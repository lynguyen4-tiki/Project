import json
import threading
import socket
import time
try:
    import Common.MyEnum as MyEnum
    import Common.MyParser as MyParser
except ImportError:
    import MyEnum
    import MyParser

DEBUG = True

eps = 0     # epsilon
ext = str('band50')
# init arguments
DELTA_K = 3
k = 4 + DELTA_K       # number elements in top
h1 = 1      # Coefficient of the 1st element in integrative function
h2 = 1      # Coefficient of the 2nd element in integrative function
h3 = 1      # Coefficient of the 3rd element in integrative function
session = 0 # the number of session
delta = 1   # coefficent delay
band = 30  # limit bandwidth

currentBand = 0
currentK = 0
netIn  = 0
netOut = 0

#value and name of top elements
topK = []
nameTop = []

lstSock = []
lstName = []

#to check whether an user connnects to
bUserConnect = False

IP_SERVER  = 'localhost'
PORT_NODE = 9407
PORT_USER = 7021
MAX_NUMBER_NODE = 50
DELTA_BAND = int(band / 10)
DELTA_EPS = 1
FILE_MON_NET = 'NetWorkLoad_'+ ext+'.dat'

NUM_MONITOR = 120

#interval to update network load
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

def sendEPS(value:int):
    printTop()
    if (DEBUG):
        print('eps = %d' %(value))
    global lockLst
    data = createMessage('', {'-type': MyEnum.MonNode.SERVER_SET_ARG.value})
    data = createMessage(data, {'-eps': value})
    for s in lstSock:
        try:
            s.sendall(bytes(data.encode()))
            addNetworkOut(len(data))
        except socket.error:
            pass

def saveNetworkLoad(currentBand, eps):
    tmp = eps
    if (tmp <= DELTA_EPS):
        tmp = 0
    with open(FILE_MON_NET, 'a') as f:
        f.write(str(currentBand) + ' ' + str(tmp) + '\n')

def monNetwork():
    global lockNetIn
    global lockNetOut
    global netIn
    global netOut
    global eps
    global countNode
    countTime = 0
    BOUND_RESTART = 3

    oldEps = 0
    countCir = 0 # count the number circle that total of network is greater than the bandwidth limit
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
            print('netIn = %.2f _________ netOut = %.2f_____eps = %d' %(nIn, nOut, eps) )

        if (countNode > 0):
            countTime += 1
            print('CountTime = %d' %(countTime))
            if (countTime <= NUM_MONITOR):
                saveNetworkLoad(int(nIn) + int(nOut), eps)


        if (nIn + nOut < band - DELTA_BAND):
            countCir += 1
            if (countCir >= BOUND_RESTART):
                countCir = 0
                if (eps <= DELTA_EPS):
                    eps = DELTA_EPS
                    oldEps = 0
                    countCir = 0
                    continue
                if (eps - oldEps <= DELTA_EPS):
                    oldEps = eps
                    eps = int(eps / 2)
                    if (eps < DELTA_EPS):
                        eps = DELTA_EPS
                        sendEPS(0)
                    else:
                        sendEPS(eps)
                    continue
                eps = int ((eps + oldEps) / 2)
                sendEPS(eps)

        elif (nIn + nOut > band + DELTA_BAND):
            countCir -= 1
            if (countCir < -BOUND_RESTART):
                countCir = 0
                oldEps = eps
                eps *= 2
                sendEPS(eps)
        else:
            if (countCir < 0):
                countCir += 1
            elif (countCir > 0):
                countCir -= 1

################################################################################
def swap(i:int, j:int):
    tmp = topK[i]
    topK[i] = topK[j]
    topK[j] = tmp

    tmp = nameTop[i]
    nameTop[i] = nameTop[j]
    nameTop[j] = tmp

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

def sendAllNode(data: str):
    global lockLst
    for s in lstSock:
        try:
            s.sendall(bytes(data.encode()))
            addNetworkOut(len(data))
        except socket.error:
            pass

def forceGetData(bound:int):
    if (eps <= DELTA_EPS):
        return
    data = createMessage('', {'-type':MyEnum.MonNode.SERVER_GET_DATA.value})
    data = createMessage(data, {'-bound':bound})
    sendAllNode(data)

def init():
    global serverForNode, serverForUser
    global lockCount, lockLst, lockTop, lockNetIn, lockNetOut
    global parser

    for i in range(k):
        topK.append(0)
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

    #init argument parser
    parser = MyParser.createParser()

    #delete old file
    f = open(FILE_MON_NET, 'w')
    f.close()

def printTop():
    global userSock, eps, lockTop
    epsTmp = eps
    if (epsTmp <= DELTA_EPS):
        epsTmp = 0
    rTop = []
    rName = []

    lockTop.acquire()

    for i in range(k - DELTA_K):
        if (nameTop[i] == ''):
            break
        rTop.append(topK[i])
        rName.append(nameTop[i])

    lockTop.release()

    data = json.dumps([rTop, rName, epsTmp])
    if (DEBUG):
        print(data)

    try:
        userSock.sendall(data.encode())
    except Exception:
        return

################################################################################
#add new element in top
def addToTopK(value: int, name: str):
    global lockTop, currentK
    d = 0
    c = currentK - 1
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
    for i in range(k-1, g, -1):
        topK[i] = topK[i-1]
        nameTop[i] = nameTop[i-1]

    topK[g] = value
    nameTop[g] = name
    lockTop.release()
    if (currentK < k):
        currentK += 1

    printTop()

#change the order of the element in top
def changeOrderInTop(value : int, iNodeInTop: int) :
    global lockTop
    global countNode, currentK
    if (value > topK[iNodeInTop]):
        # pull up
        lockTop.acquire()
        topK[iNodeInTop] = value
        while (iNodeInTop > 0 and value > topK[iNodeInTop - 1]):
            iNodeInTop -= 1
            swap(iNodeInTop, iNodeInTop + 1)
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

        # call all node to get lastest data if the value k-th element decreases
        if (iNodeInTop == currentK - 1):
            if (currentK <= k - DELTA_K  and countNode > currentK):
                lockTop.release()
                forceGetData(0)
            else:
                topK[iNodeInTop] = 0
                nameTop[iNodeInTop] = ''
                currentK -= 1
                lockTop.release()
        else:
            lockTop.release()

        printTop()
        return

def updateTopK(value:int, name : str):
    nameNode = name
    iNodeInTop = findNodeInTop(nameNode)
    # this change doesn't effect to top
    if (iNodeInTop == -1 and value < topK[k-1]):
        return

    #an element out Top goes in Top
    if (iNodeInTop == -1 and value > topK[k - 1]):
        addToTopK(value, name)
        return

    changeOrderInTop(value, iNodeInTop)

#remove the node that is disconnected
def removeInTop(strName:str):
    global lockTop, currentK
    iIndex = findNodeInTop(strName)
    if (iIndex == -1):
        return
    lockTop.acquire()
    for i in range(iIndex, k-1):
        swap(i, i+ 1)
    topK[k-1] = 0
    nameTop[k-1] = ''
    currentK -= 1
    lockTop.release()
    if (currentK < k - DELTA_K - 1):
        forceGetData(0)
    printTop()
    pass

def updateArg(arg):
    global h1, h2, h3, band, k, lockTop, session, DELTA_BAND
    dataSend = ''

    if (arg.h1 != None):
        h1 = arg.h1[0]
        dataSend = createMessage(dataSend, {'-h1':h1})

    if (arg.h2 != None):
        h2 = arg.h2[0]
        dataSend = createMessage(dataSend, {'-h2': h2})

    if (arg.h3 != None):
        h3 = arg.h3[0]
        dataSend = createMessage(dataSend, {'-h3': h3})

    if (arg.band != None):
        band = arg.band[0]
        DELTA_BAND = int(band / 10)

    if (arg.k != None):
        newK = arg.k[0]
        newK += DELTA_K
        if (newK < k):
            lockTop.acquire()
            for i in  range(k - newK):
                topK.pop(newK)
                nameTop.pop(newK)
            k = newK
            lockTop.release()
        if (newK > k):
            lockTop.acquire()
            for i in range(newK - k):
                topK.append(0)
                nameTop.append('')
            k = newK
            lockTop.release()
            if (dataSend == ''):
                forceGetData(0)
                return

    if (dataSend != ''):
        session += 1
        dataSend = createMessage(dataSend, {'-ses' : session})
        dataSend = createMessage(dataSend, {'-type': MyEnum.MonNode.SERVER_SET_ARG.value})
        sendAllNode(dataSend)

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
        dataSend = createMessage(dataSend, {'-bound': topK[k - 1]})
        tmp = eps
        if (eps <= DELTA_EPS):
            tmp = 0
        dataSend = createMessage(dataSend, {'-eps': tmp})
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
                        updateTopK(nodeValue, nameNode)
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
        lockLst.acquire()
        lstSock.remove(s)
        lstName.remove(nameNode)
        removeInTop(nameNode)
        lockLst.release()

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
    printTop()
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
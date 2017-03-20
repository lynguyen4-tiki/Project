try:
    import Common.MyEnum as MyEnum
    import Common.MyParser as MyParser
except ImportError:
    import MyEnum
    import MyParser
import socket
import json
import threading
import os

IP_SERVER = 'localhost'
PORT_USER = 7021

HELP = """
____________________________________________________________
Chose one option in following options:
    1. Send arguments to server
    2. Show current top k
    3. Quit
____________________________________________________________
"""

bShow = False
bStop = False
eps = -1

h1 = 1
h2 = 1
h3 = 1
band = 10
k = 5
nameTop = []
valueTop = []

################################################################################
def init():
    for i in range(k):
        nameTop.append('')
        valueTop.append(0)

def createMessage(strRoot = '', arg = {}):
    strResult = str(strRoot)
    for k, v in arg.items():
        strResult = strResult + ' ' + str(k) + ' ' + str(v)

    return strResult

def inputArgs():
    global sock
    os.system('clear')
    print('Integrative Function V = h1 * CPU + h2 * RAM + h3 * MEM')
    print('input -1 if ignore the argument:')
    try:
        h1 = int(input('h1 = '))
        h2 = int(input('h2 = '))
        h3 = int(input('h3 = '))
        band = int(input('bandwidth limit: '))
        k = int(input('Number elements in top: '))
    except Exception:
        return
    dataSend = ''
    if (h1 >= 0):
        dataSend = createMessage(dataSend, {'-h1':h1})

    if (h2 >= 0):
        dataSend = createMessage(dataSend, {'-h2':h2})

    if (h3 >= 0):
        dataSend = createMessage(dataSend, {'-h3':h3})

    if (band > 0):
        dataSend = createMessage(dataSend, {'-band':band})

    if (k > 0):
        dataSend = createMessage(dataSend, {'-k':k})

    if (dataSend == ''):
        return

    dataSend = createMessage(dataSend, {'-type':MyEnum.User.USER_SET_ARG.value})
    try:
        sock.sendall(dataSend.encode())
    except socket.error:
        return

def showTop():
    global nameTop, bShow
    global valueTop
    if (not bShow):
        return
    os.system('clear')
    if (eps == -1):
        print('eps = ***')
    else:
        print('eps = %d' %(eps))
    for i in range(len(nameTop)):
        if (nameTop[i] == ''):
            break
        print('%d. %10.2f : %s' %(i + 1, valueTop[i], nameTop[i]))

    print('')
    print('Press "s" to stop showing')

def updateTopK(data):
    global  bShow, nameTop, valueTop, eps

    try:
        data = json.loads(data)
        valueTop = data[0]
        nameTop = data[1]
        eps = data[2]
        showTop()
    except Exception:
        pass


################################################################################
def listenUser():
    global bShow, bStop

    os.system('clear')
    print(HELP)

    while 1:
        if (not bShow):
            os.system('clear')
            print(HELP)
        try:
            option = str(input('Your choice: '))
        except Exception:
            continue
        if (option == '1'):
            inputArgs()
        elif (option == '2'):
            bShow = True
            showTop()
        elif (option == 's' or option == 'S'):
            bShow = False
        elif (option == '3'):
            bStop = True
            return


def listenServer(sock: socket.socket):
    global bStop
    try:
        while (not bStop):
            dataRecv = sock.recv(1024).decode()
            if (dataRecv == ''):
                return
            updateTopK(dataRecv)

    except socket.error:
        return
    finally:
        sock.close()

################################################################################
init()
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (IP_SERVER, PORT_USER)
    sock.connect(server_address)

    parser = MyParser.createParser()

    thUser = threading.Thread(target= listenUser, args=())
    thServer = threading.Thread(target= listenServer, args=(sock,))
    thUser.start()
    thServer.start()

    thUser.join()
    thServer.join()
except Exception:
    pass

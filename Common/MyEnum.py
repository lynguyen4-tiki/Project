import enum

class User(enum.Enum):
    USER_SET_K = 1
    USER_SET_ARG = 2
    USER_SET_BANDWIDTH = 3
    USER_GET_TOP = 4

class MonNode(enum.Enum):
    NODE_SET_NAME = 1
    NODE_SET_DATA = 2
    SERVER_SET_ARG = 3
    SERVER_GET_DATA = 4
    DATA_GEN_AUTO = 5
    DATA_FROM_INFLUXDB = 6
    DATA_REALTIME = 7
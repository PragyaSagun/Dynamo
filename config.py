import random
import math
''' Dictionary containing read write ports for all the threads to simulate systems
    Format :- ID : (IP,READ_PORT,WRITE_PORT)'''
CONFIG = {
    "A" : ('localhost',1024,2000),
    "B" : ('localhost',1234,2345),
    "C" : ('localhost',1025,2001),
    "D": ('localhost',5432,5000),
    "E" : ('localhost',6000,6001)
}

PORT_TO_ID = {
    1024:"A",
    1234:"B",
    1025:"C",
    5432:"D",
    6000:"E"
}

CLIENTS = {
    1: 4444,
    2: 4800,
    3: 5000
}

REQUESTS = {}
HISTORY = {}
INF=2**10000
MAX_RETRIES=3
def generate_random_number():
    return random.randint(0,INF)
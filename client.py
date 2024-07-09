import socket
import pickle
from Request import Request
import time
from Messaging import Messaging
from config import  PORT_TO_ID,CONFIG,CLIENTS,MAX_RETRIES,INF
import random
from VectorClock import  VectorClock
class Client:

    def __init__(self,id):
        self.id = id
        self.port = CLIENTS[id]
        self.request = {}
        self.datastore={}
        self.nodes = ["A","B","C","D","E"]
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('localhost', self.port))

    #Current logic for business side reconcilaiton is getting the maximum of two keys
    def perform_semantic_reconcilation(self,data):
        result = -INF
        vc=VectorClock()
        if len(data)>1:
            clocks=[]
            for value,clock in data:
                result=max(result,value)
                clocks.append(clock)
            return (result,vc.converge(clocks))
        else:
            return (data[0][0],data[0][1])

    def get_req(self,key):
        cur_time = time.time()
        return self.get(key,0)
        #print(str(time.time()-cur_time))

    def get(self,key,retries=0):
        while retries < MAX_RETRIES:
            get_request = Request("GET",key,None,None,self.port)
            dynamo_node = self.nodes[random.randint(0,len(self.nodes)-1)]
            self.socket.settimeout(5)
            Messaging.send_message(self,dynamo_node,get_request)
            try:
                while True:
                    data,addr = self.socket.recvfrom(40960)
                    if data:
                        data = pickle.loads(data)
                        if(data=="FAILURE"):
                         #   print("FAILURE")
                            return -INF
                        result,self.datastore[key]=self.perform_semantic_reconcilation(data)
                        #print(str(result)+" "+str(self.datastore[key].clock))
                        self.socket.settimeout(None)
                        return result

            except Exception:
                self.socket.settimeout(None)
                retries+=1

    def put_req(self,key,value):
        cur_time = time.time()
        return self.put(key,value,0)
        #print(str(time.time()-cur_time))


    def put(self,key,value,retries=0):
        while retries < MAX_RETRIES:
            dynamo_node = self.nodes[random.randint(0, len(self.nodes)-1)]
            vc = VectorClock()
            vc.update(dynamo_node,0)
            value1 = (value,self.datastore.get(key,vc))
            #print("Client value is "+str(value1))
            put_request = Request("PUT",key,value1,None,self.port)

            self.socket.settimeout(5)
            Messaging.send_message(self, dynamo_node, put_request)
            try:
                while True:
                    data, addr = self.socket.recvfrom(40960)
                    if data:
                        self.socket.settimeout(None)
                        return pickle.loads(data)
            except Exception:
                self.socket.settimeout(None)
                retries += 1

    def kill(self,node=None):
        if node is None:
            node = self.nodes[random.randint(0,len(self.nodes)-1)]
        kill_switch = Request("EXIT",None)
        Messaging.send_message(self, node, kill_switch)
        return node

    def resurrect(self,node):
        dragonsblood = Request("REVIVE", None)
        Messaging.send_message(self, node, dragonsblood)

# client = Client(1)
# client.put_req('x',1)
# time.sleep(1)
# client.get_req('x')
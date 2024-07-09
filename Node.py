from socket import socket,AF_INET,SOCK_DGRAM
import time
from queue import Queue
from Request import Request
from Messaging import Messaging
import pickle
from threading import Thread,get_ident
from ConsistentHashRing import ConsistentHashRing
from config import CONFIG,PORT_TO_ID,REQUESTS,HISTORY,generate_random_number
from VectorClock import VectorClock
from copy import deepcopy
import random
import math
MAX_MESSAGE_SIZE=40960
class Node(Thread):
    def __init__(self,id):
        self.vector_clock = VectorClock()
        #state of the process, 1 means alive, 0 means dead
        self.state=1
        self.message_queue=Queue()
        self.kv={}
        self.count=0
        self.vector_clock.update(id, self.count)
        self.id=id
        self.hash_ring=ConsistentHashRing.getInstance()
        self.hash_ring.add_node(self)
        self.read_port = CONFIG[self.id][1]
        self.write_port = CONFIG[self.id][2]
        self.ip = CONFIG[self.id][0]
        self.socket = socket(AF_INET,SOCK_DGRAM)
        self.socket.bind((self.ip,self.read_port))
        self.pending_reads={}
        self.pending_writes={}
        self.N=3
        self.R=1
        self.W=2
        self.failed_nodes=[]
        self.sync_kv={}
        self.check_for_sync=[]
        self.back=Thread(target=self.background_process)
        self.history={}
        Thread.__init__(self)
        self.back.start()

    def get_sequence_no(self):
        self.count+=1
        return self.count

    def perform_antientropy(self):
        for key in self.kv:
            preference_list = self.hash_ring.get_node(key)
            for node in preference_list:
                sync_message = Request("ENTROPY",key,self.kv[key])
                Messaging.send_message(self,node.id,sync_message)

    def antientropy(self,*data):
        dict=data[0]
        if dict.key not in self.kv:
            self.kv[dict.key]=dict.value
        else:
            list_other = self.kv[dict.key]
            list_other+=dict.value
            self.kv[dict.key]=self.perform_syntactic_reconcilation(list_other)

    def handoff(self):
        for node in self.check_for_sync:
            keys = list(self.sync_kv[node].keys())
            values = list(self.sync_kv[node].values())
            synchronize = Request("SYNC",keys,values,generate_random_number())
            Messaging.send_message(self,node,synchronize)

    def synchronize(self,msg):
        keys = msg.key
        values=msg.value
        for i in range(len(keys)):
            if keys[i] not in self.kv:
                self.kv[keys[i]]=[]
            self.kv[keys[i]]+=values[i]
        for key in keys:
            self.kv[key] = self.perform_syntactic_reconcilation(self.kv[key])
        #print(self.id+" "+str(self.kv))

    def checkIfAlive(self):
        for node in self.failed_nodes:
            ping = Request("PING", None)
            Messaging.send_message(self, node, ping)

    def background_process(self):
        try:
            while True:
                if self.state == 1:
                    self.handoff()
                    self.checkIfAlive()
                    #self.perform_antientropy()
                    time.sleep(1)
        except Exception:
            self.background_process()

    def perform_put(self,*data):
        dict = data[0]
        key = dict.key
        preference_list = self.hash_ring.get_node(key,self.failed_nodes)
        if(self not in preference_list):
            coordinator = preference_list[0].id
            dict = Request("FORWARD-PUT",dict.key,dict.value,generate_random_number(),dict.client)
            Messaging.send_message(self,coordinator,dict)
            time.sleep(3)
            if REQUESTS.get(dict.request,False)!=True:
                #print("Timedout PUT")
                dict.action="PUT"
                dict.request=generate_random_number()
                self.socket.settimeout(None)
                self.failed_nodes.append(coordinator)
                self.perform_put(dict)
        else:
            self.vector_clock.update(self.id,self.get_sequence_no())
            metadata = deepcopy(self.vector_clock)
            if dict.value[1] > metadata:
                metadata = dict.value[1]
            dict.value = (dict.value[0],metadata)
            dict.request=generate_random_number()
            Messaging.broadcast_put(self,preference_list,dict)

    def perform_syntactic_reconcilation(self,list):
        dict = {}
        clocks=[]
        #print("list is "+str(list)+" "+self.id)
        for value,clock in list:
            dict[clock]=value
            clocks.append(clock)
        result = self.vector_clock.combine(clocks)
        final_result=[]
        for clock in result:
            final_result.append((dict[clock],clock))
        return final_result

    def perform_store(self,data):
        time.sleep(random.randint(0,1000)/1000)
        dict = data[0]
        addr=data[1]
        self.vector_clock.update(self.id,self.get_sequence_no())
        self.vector_clock=self.vector_clock.converge([self.vector_clock,dict.value[1]])
        if dict.key not in self.kv:
            self.kv[dict.key]=list()
        self.kv[dict.key].append(dict.value)
        self.kv[dict.key]=self.perform_syntactic_reconcilation(self.kv[dict.key])
        dict = Request("ACK-PUT",None,None,dict.request)
        Messaging.send_message(self,PORT_TO_ID[addr[1]],dict)

    def perform_get(self,dict):
        key = dict.key
        preference_list = self.hash_ring.get_node(key,self.failed_nodes)
        if(self not in preference_list):
            coordinator = preference_list[0].id
            dict = Request("FORWARD-GET", dict.key,dict.value,generate_random_number(),dict.client)
            Messaging.send_message(self, coordinator, dict)
            time.sleep(3)
            if REQUESTS.get(dict.request,False)!=True:
                #print("Timedout GET")
                dict.action="GET"
                dict.request=generate_random_number()
                self.failed_nodes.append(coordinator)
                self.perform_get(dict)
        else:
            dict.request = generate_random_number()
            Messaging.broadcast_get(self,preference_list,dict)

    def retreive_key(self,*data):
        time.sleep(random.randint(0, 1000) / 1000)
        dict = data[0]
        from_node=data[1]
        val = self.kv.get(dict.key,None)
        response = Request("ACK-GET",dict.key,val,dict.request)
        Messaging.send_message(self,from_node,response)

    #Utility #Print function
    def string(self,dict):
        temp="{"
        for key in dict:
            temp=temp+","+str(key)+":"
            temp2="["
            for val in dict[key]:
                 temp2+=str(val[0])+","+str(val[1].clock)+","
            temp2+="]"
            temp+=temp2
        temp+="}"
        return temp
# Message gets uncompressed first
    def run(self):
        try:
            while True:
                data,addr = self.socket.recvfrom(MAX_MESSAGE_SIZE)
                dict = pickle.loads(data)
                # #print(self.id + " received data from" + str(addr[1]))
                if self.state==1:
                    if dict.action=="PUT":
                        #print(self.id+" received PUT "+str(dict.key)+" "+str(dict.value)+" "+str(dict.client))
                        thread1 = Thread(target=self.perform_put,args=(dict,))
                        thread1.start()
                    if dict.action=="STORE":
                        #print(self.id + " received STORE"+str(dict.key)+" "+str(dict.value[0]))
                        thread2 = Thread(target=self.perform_store,args=([dict,addr],))
                        thread2.start()
                        #print(self.id+" "+self.string(self.kv))
                    if dict.action=="GET":
                        #print(self.id+" Received Get")
                        thread3 = Thread(target=self.perform_get,args=(dict,))
                        thread3.start()
                    if dict.action=="FETCH":
                        #print(self.id+" Received Fetch")
                        thread4 = Thread(target=self.retreive_key,args=[dict,PORT_TO_ID[addr[1]]])
                        thread4.start()
                    if dict.action=="EXIT":
                        #print(self.id+" Will Fail now")
                        self.state=0
                    if dict.action=="FORWARD-PUT":
                        #print(self.id+"Received Forward")
                        dict.action="PUT"
                        response = Request("ACK-FORWARD-PUT",None,None,dict.request)
                        self.socket.sendto(pickle.dumps(response),addr)
                        thread5 = Thread(target=self.perform_put, args=(dict,))
                        thread5.start()
                        #self.perform_put(dict)
                    if dict.action=="FORWARD-GET":
                        #print(self.id+"Received Forward")
                        dict.action="GET"
                        response = Request("ACK-FORWARD-GET",None,None,dict.request)
                        self.socket.sendto(pickle.dumps(response),addr)
                        threada = Thread(target=self.perform_get,args=[dict])
                        threada.start()
                    if dict.action=="PING":
                        #print(self.id+"Received Ping")
                        response = Request("PONG", None)
                        Messaging.send_message(self,PORT_TO_ID[addr[1]],response)
                    if dict.action=="ACK-FORWARD-PUT":
                        #print("Received forward PUT ack")
                        REQUESTS[dict.request]=True
                    if dict.action=="ACK-FORWARD-GET":
                        #print("Received forward Get ack")
                        REQUESTS[dict.request]=True
                    if dict.action=="ACK-PUT":
                        #print("PUT ACK received by "+self.id +" from "+ PORT_TO_ID[addr[1]])
                        if dict.request not in HISTORY:
                            HISTORY[dict.request] = set()
                        HISTORY[dict.request].add(PORT_TO_ID[addr[1]])
                    if dict.action=="ACK-GET":
                        #print("GET ACK received by "+self.id +" from "+ PORT_TO_ID[addr[1]])
                        if dict.request not in HISTORY:
                            HISTORY[dict.request] = list()
                        HISTORY[dict.request].append((PORT_TO_ID[addr[1]],dict.value))

                    if dict.action=="SYNC":
                        #print(self.id+"Received handoff Sync")
                        thread10 = Thread(target=self.synchronize,args=[dict])
                        thread10.start()
                        response=Request("SYNC-ACK",None)
                        Messaging.send_message(self,PORT_TO_ID[addr[1]],response)

                    if (dict.action == "SYNC-ACK"):
                        #print(self.id + " Synced " + PORT_TO_ID[addr[1]])
                        self.sync_kv.pop(PORT_TO_ID[addr[1]], None)
                        self.check_for_sync.remove(PORT_TO_ID[addr[1]])

                    if (dict.action == "PONG"):
                        self.failed_nodes.remove(PORT_TO_ID[addr[1]])
                        #print("node" + str(PORT_TO_ID[addr[1]])+" is alive")
                        #print("Failed nodes=" + str(self.failed_nodes))

                    if (dict.action == "ENTROPY"):
                        ##print(self.id+" received entropy")
                        threadb = Thread(target=self.antientropy,args=[dict])
                        threadb.start()

                else:
                    if dict.action=="REVIVE":
                        #print(self.id+" is reviving")
                        self.state=1
        except Exception:
            self.run()




node1 = Node("A")
node2 = Node("B")
node3 = Node("C")
node4 = Node("D")
node5 = Node("E")
node1.start()
node2.start()
node3.start()
node4.start()
node5.start()







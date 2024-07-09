import hashlib
from bisect import bisect
import threading
class ConsistentHashRing:
    __instance = None
    def __init__(self):
        if ConsistentHashRing.__instance == None:
            self.dictionary = {}
            self.list=[]
            self.N=3
            self.R=2
            self.W=2
            self.virtual_nodes=3
            self.node_list=[]
            ConsistentHashRing.__instance = self
        else:
            raise Exception("Object already created")

    def get_node(self,key,removed_nodes=set(),N=3):
        hash_value = hashlib.md5(str(key).encode()).digest()
        index = bisect(self.list,hash_value)
        result_list=[]
        duplicates=set()
        while len(result_list)<N:
            if(index==len(self.list)):
                index=0
            if (self.dictionary[self.list[index]] in duplicates):
                index+=1
                continue
            if (self.dictionary[self.list[index]].id in removed_nodes):
                index+=1
                continue
            result_list.append(self.dictionary[self.list[index]])
            duplicates.add(self.dictionary[self.list[index]])
            index+=1
        return result_list

    @staticmethod
    def getInstance():
        if ConsistentHashRing.__instance ==None:
            ConsistentHashRing()
        return ConsistentHashRing.__instance

    def add_node(self,node):
        for i in range(0,self.virtual_nodes):
            id = node.id + ":"+ str(threading.get_ident())+":"+str(i)
            hash_value = hashlib.md5(id.encode()).digest()
            self.dictionary[hash_value] = node
            self.list.append(hash_value)
        self.list = sorted(self.list)
        self.node_list.append(node)


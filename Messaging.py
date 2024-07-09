from config import CONFIG,PORT_TO_ID,HISTORY,generate_random_number,REQUESTS
import pickle
import time
from Request import Request

class Messaging:

    @staticmethod
    def send_message(from_proc,to_proc,msg):
        compressed_msg = pickle.dumps(msg)
        from_proc.socket.sendto(compressed_msg,(CONFIG[to_proc][0],CONFIG[to_proc][1]))

    @staticmethod
    def broadcast_put(from_node,node_list,msg):
        if(len(node_list)==0):
            if len(HISTORY.get(msg.request,set())) <from_node.W:
                from_node.socket.sendto(pickle.dumps("FAILURE"),('localhost',msg.client))
            elif REQUESTS.get(msg.request,False)==True:
                from_node.socket.sendto(pickle.dumps("SUCCESS"),('localhost',msg.client))
            return

        msg = Request("STORE",msg.key,msg.value,generate_random_number(),msg.client)
        nodes=[]
        for node in node_list:
            nodes.append(node.id)
            #print("Preference list="+node.id+msg.action + "from "+from_node.id +" "+str(msg.key)+":"+str(msg.value[0]))
            Messaging.send_message(from_node,node.id,msg)
        cur_time = time.time()
        while int(time.time() - cur_time) < 3:
            if not REQUESTS.get(msg.request,False) and len(HISTORY.get(msg.request,set())) >=from_node.W:
                #send client success message
                from_node.socket.sendto(pickle.dumps("SUCCESS"),('localhost',msg.client))
                REQUESTS[msg.request]=True
        HISTORY.get(msg.request,set()).add(from_node.id)
        failed_nodes = set(nodes) - HISTORY[msg.request]
        from_node.failed_nodes = from_node.failed_nodes + list(failed_nodes)
        #print("FAILED NODES "+str(from_node.failed_nodes))
        Messaging.retry_put_request(from_node, failed_nodes, msg, HISTORY[msg.request])

    @staticmethod
    def broadcast_get(from_node,node_list,msg):
        if(len(node_list)==0):
            if len(HISTORY.get(msg.request,set())) <from_node.W:
                from_node.socket.sendto(pickle.dumps("FAILURE"),('localhost',msg.client))
            return
        msg = Request("FETCH",msg.key,msg.value,generate_random_number(),msg.client)
        nodes=[]
        for node in node_list:
            nodes.append(node.id)
            #print("Preference list="+node.id+msg.action)
            Messaging.send_message(from_node,node.id,msg)
        cur_time = time.time()
        while int(time.time() - cur_time) < 3:
            if not REQUESTS.get(msg.request,False) and len(HISTORY.get(msg.request,set()))>=from_node.R:
                result = list()
                for id,val in HISTORY[msg.request]:
                    if val !=None:
                        result+=val
                #print([(number,vector.clock) for number,vector in result])
                result= from_node.perform_syntactic_reconcilation(result)
                from_node.socket.sendto(pickle.dumps(result),('localhost',msg.client))
                #for num,clocks in result:
                 #   print(str(msg.key)+" "+str(num)+" "+str(clocks.clock))
                REQUESTS[msg.request]=True
        readers = set([id for id,val in HISTORY[msg.request]])
        failed_nodes = set(nodes)-readers
        from_node.failed_nodes+=list(failed_nodes)
        Messaging.retry_get_request(from_node,failed_nodes,msg,readers)

    @staticmethod
    def retry_put_request(from_node,failed_nodes,msg,writers):
       if(len(failed_nodes)==0):
            return
       preference_list = from_node.hash_ring.get_node(msg.key,from_node.failed_nodes)
       new_preference_list = []
       for node in preference_list:
           if node.id not in writers:
               new_preference_list.append(node)
       #print("NEW LIST="+str(new_preference_list))
       for node in new_preference_list:
           node.check_for_sync=node.check_for_sync+list(failed_nodes)
           for fail in failed_nodes:
               if fail not in node.sync_kv:
                   node.sync_kv[fail]={}
               if msg.key not in node.sync_kv[fail]:
                   node.sync_kv[fail][msg.key]=[]
               node.sync_kv[fail][msg.key].append(msg.value)
       Messaging.broadcast_put(from_node,new_preference_list,msg)

    @staticmethod
    def retry_get_request(from_node,failed_nodes,msg,writers):
       if(len(failed_nodes)==0):
            return set()
       preference_list = from_node.hash_ring.get_node(msg.key,from_node.failed_nodes)
       new_preference_list = []
       for node in preference_list:
           if node.id not in writers:
               new_preference_list.append(node)
       #print("NEW LIST="+str(new_preference_list))
       return Messaging.broadcast_get(from_node,new_preference_list,msg)



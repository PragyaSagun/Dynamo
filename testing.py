import unittest
from client import Client
import time
import numpy as np
#Simple test with 1 client and 1 get put operation
import random

read=[]
write=[]

##############################Tests with synchronous calls.###############################################
def test_happyGetSetTest():
    status = client1.put_req('x', 1)
    print(status)
    time.sleep(1)
    num=client1.get_req('x')
    print(num==1)

#Test with multiple gets and puts with delays having one client only
def test_multipleGetSetDelay():
    client1.put_req('x',1)
    client1.put_req('y', 2)
    client1.put_req('z', 3)
    time.sleep(1)
    x=client1.get_req('x')
    y=client1.get_req('y')
    z=client1.get_req('z')
    print(x==1)
    print(y == 2)
    print(z == 3)

#Testing with same keys multiple values. According to semantic reconcilation, answer should be the largest key
def test_multipleGetSetDelaySameKey():
    client1.put_req('x',1)
    client1.put_req('x',2)
    client1.put_req('x',3)
    time.sleep(1)
    x=client1.get_req('x')
    y=client1.get_req('x')
    z=client1.get_req('x')
    print(x==3)
    print(y==3)
    print(z==3)

def test_loadtestingUniqueKeys():
    client2 = Client(2)
    client3 = Client(3)
    for i in range(100):
        client1 = Client(1)
        cur = time.time()
        client1.put_req(str(i),i)
        write.append(time.time()-cur)
        time.sleep(1)
        del client1
    time.sleep(1)
    for i in range(100):
        client1 = Client(1)
        cur = time.time()
        r=client1.get_req(str(i))
        read.append(time.time() - cur)
        time.sleep(1)
        del client1

    print(str(np.median(write))+" "+str(np.max(write))+" "+str(np.min(write)))
    print(str(np.median(read)) + " " + str(np.max(read)) + " " + str(np.min(read)))


#Fail a process at random and get latency
def test_randomprocessSystemFailure():
    kill=False
    killed=""
    for i in range(100):
        client1 = Client(1)
        cur = time.time()
        if kill==True and i%10==0:
            client1.resurrect(killed)
            kill=False
        elif kill==False and i%10==0:
            killed=client1.kill()
            kill=True
        else:
            client1.put_req(str(i),i)
            write.append(time.time()-cur)
        time.sleep(1)
        del client1
    time.sleep(5)
    for i in range(100):
        client1 = Client(1)
        cur = time.time()
        r=client1.get_req(str(i))
        read.append(time.time() - cur)
        time.sleep(1)
        del client1

    print(str(np.mean(write))+" "+str(np.median(write))+" "+str(np.max(write))+" "+str(np.min(write)))
    print(str(np.mean(read))+" "+str(np.median(read)) + " " + str(np.max(read)) + " " + str(np.min(read)))
############################################ Tests simulating eventual consistency ######################################################test

def init():
    client2 = Client(2)
    for i in range(2,1000):
        client2.put_req('x'+str(i),i)

def get_eventual_consistency_params():
    init()
    times=[]
    counts=[]
    i=0
    client1 = Client(1)
    while i <1000:
        count=1
        key = 'x1'
        val=i
        client1.put_req(key, val)
        while(client1.get_req('x1')!=i):
            count+=1
        counts.append(count)
        i+=1
    print(counts)

def eventual_consistency_params_randomized_failure():
    init()
    counts=[]
    i=0
    client1 = Client(1)
    failures_recoveries =[random.randint(0,1000) for i in range(10)]
    failures_recoveries=set(sorted(failures_recoveries))
    kill=False
    killed=""
    while i <1000:
        print(i)
        if i in failures_recoveries:
            if kill == False:
                killed=client1.kill()
                kill=True
            else:
                client1.resurrect(killed)
                kill=False
            i+=1
        else:
            count=1
            key = 'x1'
            val=i
            client1.put_req(key, val)
            while(client1.get_req('x1')!=i):
                count+=1
            counts.append(count)
            i+=1
    print(counts)



#test_randomprocessSystemFailure()
#get_eventual_consistency_params()
#test_loadtestingUniqueKeys()
#test_happyGetSetTest()
#test_multipleGetSetDelay()
#test_multipleGetSetDelaySameKey()
eventual_consistency_params_randomized_failure()
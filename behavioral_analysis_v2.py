#!/usr/bin/env python
# -*- coding: utf-8 -*-

src_path = "dataset/"

import csv
import collections
import pprint
import subprocess
#import metis
import numpy as np
import pandas as pd
import networkx as nx
from datetime import datetime
from datetime import timedelta
from dateutil.parser import parse
from Purchase import purchases

#print (purchases[0].seller.buyer.date) 16853
#print (datetime.strptime(purchases[0].date, '%Y-%m-%d') + timedelta(days=30))

print ("Processing Purchase data, please wait... \n")

# transaction records - directed edges
transaction = []
for i in range(len(purchases)): # loading data from import
    trans_dict = {}
    trans_dict['seller'] = purchases[i].seller
    trans_dict['buyer'] = purchases[i].buyer
    trans_dict['date'] = purchases[i].date
    transaction.append(trans_dict)

datetime_list = [] # list of time stamps
for i in range(len(purchases)):
    datetime_list.append(datetime.strptime(purchases[i].date, '%Y-%m-%d'))
time_length = (max(datetime_list) - min(datetime_list)) # 374 = 34 * 11

time_interval = [] # create time interval for snapshots
tempDate = min(datetime_list)
while tempDate < max(datetime_list):
    time_interval.append(tempDate)
    tempDate += timedelta(days=17) # hard coded
time_interval.append(max(datetime_list)) # edge

# sort transactions by date return a sorted list
transaction_sorted = sorted(transaction, key=lambda transaction: transaction['date'])

def partition(values, indices): # split list in to sublists based on another list
    idx = 0
    for index in indices:
        sublist = []
        while idx < len(values) and datetime.strptime(values[idx]['date'], '%Y-%m-%d') <= index:
            sublist.append(values[idx])
            idx += 1
        if sublist:
            yield sublist

def cluster(d): # Clustering the keys of a dict according to its values
    clusters = {}
    for key, val in d.items(): # python3 removed iteritems()
        clusters.setdefault(val, []).append(key)
    return clusters
#print (cluster(Community))

Node = [] # all node name set
ClusterNodeT = [] # node name set clustered by time
Community = {} # dict of nodes with communities assigned
dictList = [] # list of dicts of nodes clustered for each snapshot

"""
# Loading CSV
for i in range(1, 10):
    Comm = {}
    file_path = "%sgraph0%s.csv"%(src_path, i)
    with open(file_path, "rb") as fl:
        reader = csv.reader(fl, delimiter=',')
        for row in reader:
            Comm[row[0]] = row[3]
    Community.append(Comm)
#print (Community[1])

for i in range(1, 10): # now only consider from 01 to 09
    file_path = "%s20170%s.csv"%(src_path, i)
    with open(file_path, "rb") as fp:
        root = csv.reader(fp, delimiter=',')
        result = collections.defaultdict(list)

        # ignore the header
        #next(root, None) 

        n = [] # node name set
        for row in root:
            nameS = row[1]
            nameB = row[3]
            if(nameS not in Node):
                Node.append(nameS)
            if(nameB not in Node):
                Node.append(nameB)
            if(nameS not in n):
                n.append(nameS)
            if(nameB not in n):
                n.append(nameB)
        #print (Node)
        Cluster.append(n)
"""

# dictionary of node name and metis
for i in range(len(transaction_sorted)):
    if not transaction_sorted[i]['seller'] in Node:
        Node.append(transaction_sorted[i]['seller'])
    if not transaction_sorted[i]['buyer'] in Node:
        Node.append(transaction_sorted[i]['buyer'])

# return list of snapshots, total=22, length=34, overlap=17
ClusterAll = list(partition(transaction_sorted, time_interval))
#print (ClusterAll)
"""
METIS GRAPH File Characteristics:
ASCII
a graph of N nodes is stored in a file of N+1 lines;
the first line lists the number of nodes and the number of edges;
If the first line contains more than two values, the extra values indicate the weights;
each subsequent line lists the "neighbors" of a node;
comment lines begin with a "%" sign;
"""
# Generate .metis files snapshots for community discovery
for j in range(len(ClusterAll)):
    Cluster = ClusterAll[j] # transaction list currently
    ClusterNode = [] # node name set currently
    for i in range(len(Cluster)):
        if not Cluster[i]['seller'] in ClusterNode:
            ClusterNode.append(Cluster[i]['seller'])
        if not Cluster[i]['buyer'] in ClusterNode:
            ClusterNode.append(Cluster[i]['buyer'])
    ClusterNodeT.append(ClusterNode)

    file_path = "%sPurchase%s.metis"%(src_path, j)
    with open(file_path, 'w') as f:
        f.write("%s %s\n" % (len(ClusterNode), len(Cluster)))
        for node in ClusterNode:
            node_neighbors = []
            for i in range(len(Cluster)):
                if Cluster[i]['seller'] == node \
                and Cluster[i]['buyer'] not in node_neighbors:
                    node_neighbors.append(Cluster[i]['buyer'])
                    curNum = ClusterNode.index(Cluster[i]['buyer']) + 1
                    f.write("%d " % curNum)
                elif Cluster[i]['buyer'] == node \
                and Cluster[i]['seller'] not in node_neighbors:
                    node_neighbors.append(Cluster[i]['seller'])
                    curNum = ClusterNode.index(Cluster[i]['seller']) + 1
                    f.write("%d " % curNum)
            if not node == ClusterNode[len(ClusterNode) - 1]:
                f.write("\n")

    # insert a command line command to run clustering alg
    subprocess.check_output(['./mlrmcl', file_path]) #mlrmcl

    # extract Community labels, starts from 0
    file_path = "%sPurchase%s.metis.c1000.i2.0.b0.5"%(src_path, j)
    with open(file_path, 'r') as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    Community = {}
    for i in range(len(content)):
        Community[ClusterNode[i]] = content[i]
    # list of clustered dictionary
    dictList.append(cluster(Community)) # use cluster function

print ("Running Evolutionary Analysis, please wait... \n")

continueCount = 0 # indicate number
continueIndex = False # indicate event
kai = 50
kMergeCount = 0
kMergeIndex = False
kSplitCount = 0
kSplitIndex = False
formCount = 0
formIndex = False
formList = []
dissolveCount = 0
dissolveIndex = False
dissolveList = []
appearCount = 0
appearIndex = False
appearList = []
disappearCount = 0
disappearIndex = False
disappearList = []
joinCount = 0
joinIndex = False
joinList = []
joinListCount = []
joinListCount.append({}) # initialize for time 0
leaveCount = 0
leaveIndex = False
leaveList = []
leaveListCount = []
leaveListCount.append({}) # initialize for time 0

T_start = 4
T_end = 5
for t in range(T_start, T_end): #range(1, len(ClusterAll)-1): # snapshots 0-23
    
    print ("Events involving communities:")
    for key in dictList[t]: # clusters in each timestamp
        #print ("Continue")
        for j in dictList[t-1]:
            if set(dictList[t][key]) == set(dictList[t-1][j]):
                continueIndex = True
                continueCount += 1
                #print ("Continue event is found:")
                #print("The time is %s. The cluster is %s."%(t, key))
                #print(dictList[t][key] )
        #print ("kai-Merge")           
        for j in dictList[t]:
            if not j == key:
                for k in dictList[t+1]:
                    if len((set(dictList[t][key]) | set(dictList[t][j])) & set(dictList[t+1][k])) > \
                    kai/100.0*max(len(set(dictList[t][key]) | set(dictList[t][j])), len(dictList[t+1][k])):
                        kMergeIndex = True
                        kMergeCount += 1
                        #print("k-Merge event is found:")
                        #print("The time is %s. The cluster is %s."%(t, key))
                        #print(dictList[t][key])
        #print ("kai-Split")
        for j in dictList[t+1]:
            for k in dictList[t+1]:
                if not j == key:
                    if len((set(dictList[t+1][j]) | set(dictList[t+1][k])) & set(dictList[t][key])) > \
                    kai/100.0*max(len(set(dictList[t+1][j]) | set(dictList[t+1][k])), len(dictList[t][key])):
                        kSplitIndex = True
                        kSplitCount += 1
                        #print("k-Split event is found:")
                        #print("The time is %s. The cluster is %s."%(t, key))
                        #print(dictList[t][key])
        #print ("Form")
        indicator = False
        for j in dictList[t-1]:
            if len(set(dictList[t][key]) & set(dictList[t-1][j])) > 1:
                indicator = True
                break
        if indicator == False:
            formIndex = True
            formCount += 1
            #formList.append(key)
            #print("Form event is found:")
            #print("The time is %s. The cluster is %s."%(t, key))
        #print ("Dissolve")
        indicator = False
        for j in dictList[t+1]:
            if len(set(dictList[t][key]) & set(dictList[t+1][j])) > 1:
                indicator = True
                break            
        if indicator == False:
            dissolveIndex = True
            dissolveCount += 1
            #dissolveList.append(key)
            #print("Dissolve event is found:")
            #print("The time is %s. The cluster is %s."%(t, key))

    
    print ("Events involving individuals:")
    #print ("Appear")
    if not set(ClusterNodeT[t]) - set(ClusterNodeT[t-1]) is None:
        appearIndex = True
        appearCount += len(set(ClusterNodeT[t]) - set(ClusterNodeT[t-1]))
        #print("Appear event is found:")
        #print("The time is %s. The node list is:"%(t))
        #print(set(ClusterNodeT[t]) - set(ClusterNodeT[t-1]))
        #appearList.append(set(ClusterNodeT[t]) - set(ClusterNodeT[t-1]))
    #print ("Disappear")
    if not set(ClusterNodeT[t-1]) - set(ClusterNodeT[t]) is None:
        disappearIndex = True
        disappearCount += len(set(ClusterNodeT[t-1]) - set(ClusterNodeT[t]))
        #print("Disappear event is found:")
        #print("The time is %s. The node list is:"%(t))
        #print(set(ClusterNodeT[t-1]) - set(ClusterNodeT[t]))
        #disappearList.append(set(ClusterNodeT[t-1]) - set(ClusterNodeT[t]))
    
    #print ("Join") # The cluster should be already exist
    joinDict = {}
    for key in dictList[t]:
        joinNumber = 0
        for j in dictList[t-1]:
            if len(set(dictList[t][key]) & set(dictList[t-1][j])) > 0.5*len(set(dictList[t-1][j])):
                joinIndex = True
                joinNumber += len(set(dictList[t][key]) - set(dictList[t-1][j]))
                joinCount += len(set(dictList[t][key]) - set(dictList[t-1][j]))
                #print("Join event is found:")
                #print("The time is %s. The node list is:"%(t))
                #print(set(dictList[t][key]) - set(dictList[t-1][key]))
                #joinList.append(set(dictList[t][key]) - set(dictList[t-1][key]))
        if not joinNumber == 0:
            joinDict[key] = joinNumber
    joinListCount.append(joinDict)
    #print ("Leave") # The cluster should be already exist
    leaveDict = {}
    for key in dictList[t]:
        leaveNumber = 0
        for j in dictList[t-1]:
            if len(set(dictList[t][key]) & set(dictList[t-1][j])) > 0.5*len(set(dictList[t-1][j])):
                leaveIndex = True
                leaveNumber += len(set(dictList[t-1][j]) - set(dictList[t][key]))
                leaveCount += len(set(dictList[t-1][j]) - set(dictList[t][key]))
                #print("Leave event is found:")
                #print("The time is %s. The node list is:"%(t))
                #print(set(dictList[t-1][key]) - set(dictList[t][key]))
                #leaveList.append(set(dictList[t-1][key]) - set(dictList[t][key]))
        if not leaveNumber == 0:
            leaveDict[key] = leaveNumber
    leaveListCount.append(leaveDict)
print (joinListCount)
print (leaveListCount)
       
print ("BEHAVIORAL ANALYSIS:")

def stability_index(node, tstart, tend): # [ ) keep the same interval
    SI = 0.0
    for t in range(tstart, tend):
        for key in dictList[t]:
            if node in dictList[t][key]:
                if key in joinListCount[t-T_start+1]:
                    if key in leaveListCount[t-T_start+1]:
                        SI += 1.0 * len(dictList[t][key]) / (joinListCount[t-T_start+1][key] + leaveListCount[t-T_start+1][key])
                        print (SI, t, key, dictList[t][key])
                    else:
                        SI += 1.0 * len(dictList[t][key]) / (joinListCount[t-T_start+1][key])
                        print (SI, t, key, dictList[t][key])
                else:
                    if key in leaveListCount[t-T_start+1]:
                        SI += 1.0 * len(dictList[t][key]) / (leaveListCount[t-T_start+1][key])
                        print (SI, t, key, dictList[t][key])      
    if SI == 0:
        print ("ERROR in Stability Index: node not found!")
        return 0
    return SI

def sociability_index(node, tstart, tend): # how long node stay
    SoI = 0.0
    activity = 0.0
    leave = 0
    join = 0
    for t in range(tstart, tend):
        if node in ClusterNodeT[t]:
            activity += 1
            """
            for key in dictList[t]:
                if node in dictList[t][key]: # know which cluster node is in
                    for j in dictList[t+1]:
                        if len(set(dictList[t][]) &)
            """
    return activity

 

#print ("Stability Index")
#print (stability_index("Noone00", T_start, T_end))
#print ("Sociability Index")
#print (sociability_index("sanity", T_start, T_end))



print ("\n************ SUMMARY ************\n")

print("Events involving communities:\n")

if continueIndex == False:
    print("\tContinue event not found!")
else:
    print("\tThe number of Continue event is %s"%continueCount)
if kMergeIndex == False:
    print("\tk-Merge event not found!")
else:
    print("\tThe number of k-Merge event is %s"%kMergeCount)
if kSplitIndex == False:
    print("\tk-Split event not found!")
else:
    print("\tThe number of k-Split event is %s"%kSplitCount)
if formIndex == False:
    print("\tForm event not found!")
else:
    print("\tThe number of Form event is %s"%formCount)
    #print("\t", formList)
if dissolveIndex == False:
    print("\tDissolve event not found!")
else:
    print("\tThe number of Dissolve event is %s"%dissolveCount)
    #print("\t", dissolveList)

print("\nEvents involving individuals:\n")

if appearIndex == False:
    print("\tAppear event not found!")
else:
    print("\tThe number of Appear event is %s"%appearCount)
if disappearIndex == False:
    print("\tDisappear event not found!")
else:
    print("\tThe number of Disappear event is %s"%disappearCount)
if joinIndex == False:
    print("\tJoin event not found!")
else:
    print("\tThe number of Join event is %s"%joinCount)
if leaveIndex == False:
    print("\tLeave event not found!")
else:
    print("\tThe number of Leave event is %s"%leaveCount)

print("\nBehavioral analysis:\n")


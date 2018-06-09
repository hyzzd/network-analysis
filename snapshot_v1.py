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
    for key, val in d.iteritems():
        clusters.setdefault(val, []).append(key)
    return clusters
#print (cluster(Community))

Node = [] # all node name set
Cluster = [] # node name clustered by time
Community = [] # dict of nodes with communities assigned
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
    Cluster = ClusterAll[j]
    ClusterNode = []
    for i in range(len(Cluster)):
        if not Cluster[i]['seller'] in ClusterNode:
            ClusterNode.append(Cluster[i]['seller'])
        if not Cluster[i]['buyer'] in ClusterNode:
            ClusterNode.append(Cluster[i]['buyer'])
    #print (ClusterNode)

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
    subprocess.check_output(['./mlrmcl', file_path])            

# Community = ... 

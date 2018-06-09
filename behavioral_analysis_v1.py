#!/usr/bin/env python
# -*- coding: utf-8 -*-

src_path = "dataset/"

import csv
import collections
import pprint
#import metis
import numpy as np
import pandas as pd
import networkx as nx
from datetime import datetime
from datetime import timedelta
from dateutil.parser import parse
from Purchase import purchases

Node = [] # all node name set
Cluster = [] # node name clustered by time
Community = [] # dict of nodes with communities assigned
dictList = [] # list of dicts of nodes clustered for each snapshot

#Community = 

for t in range(0, 9): # now only consider from 01 to 09
    tempDict = {}
    for index in Cluster[t]:
        # first discovery the communities for each snapshot
        tempDict[index] = Community[t][index]
    dictList.append(cluster(tempDict))
#print (dictList[0]['0'])

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
joinListCount.append({'0': 0}) # initialize for time 0
leaveCount = 0
leaveIndex = False
leaveList = []
leaveListCount = []
leaveListCount.append({'0': 0}) # initialize for time 0

for t in range(1, 8): # snapshots 0-8
    # Events involving communities:
    for key in dictList[t]: # clusters in each timestamp
        # Continue
        for j in dictList[t-1]:
            if set(dictList[t][key]) == set(dictList[t-1][j]):
                continueIndex = True
                continueCount += 1
                #print ("Continue event is found:"
                #print("The time is %s. The cluster is %s."%(t, key))
                #print(dictList[t][key] )
        # kai-Merge           
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
        # kai-Split
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
        # Form
        indicator = False
        for j in dictList[t-1]:
            if len(set(dictList[t][key]) & set(dictList[t-1][j])) > 1:
                break
            indicator = True
        if indicator == True:
            formIndex = True
            formCount += 1
            #formList.append(key)
            #print("Form event is found:")
            #print("The time is %s. The cluster is %s."%(t, key))
        # Dissolve
        indicator = False
        for j in dictList[t+1]:
            if len(set(dictList[t][key]) & set(dictList[t+1][j])) > 1:
                break
            indicator = True
        if indicator == True:
            dissolveIndex = True
            dissolveCount += 1
            #dissolveList.append(key)
            #print("Dissolve event is found:")
            #print("The time is %s. The cluster is %s."%(t, key))

    
    # Events involving individuals:
    # Appear
    if not set(Cluster[t]) - set(Cluster[t-1]) is None:
        appearIndex = True
        appearCount += len(set(Cluster[t]) - set(Cluster[t-1]))
        #print("Appear event is found:")
        #print("The time is %s. The node list is:"%(t))
        #print(set(Cluster[t]) - set(Cluster[t-1]))
        #appearList.append(set(Cluster[t]) - set(Cluster[t-1]))
    # Disappear
    if not set(Cluster[t-1]) - set(Cluster[t]) is None:
        disappearIndex = True
        disappearCount += len(set(Cluster[t-1]) - set(Cluster[t]))
        #print("Disappear event is found:")
        #print("The time is %s. The node list is:"%(t))
        #print(set(Cluster[t-1]) - set(Cluster[t]))
        #disappearList.append(set(Cluster[t-1]) - set(Cluster[t]))
    # Join
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
        joinDict[key] = joinNumber
    joinListCount.append(joinDict)
    # Leave
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
        leaveDict[key] = leaveNumber
    leaveListCount.append(leaveDict)
                      
# BEHAVIORAL ANALYSIS:
# Stability Index
for t in range(1, 8):
    for key in dictList[t]:
        for index in dictList[t][key]:
            if not joinListCount[t][Community[t][index]] + leaveListCount[t][Community[t][index]] == 0:
                SI = 1.0 * len(dictList[t][Community[t][index]]) / (joinListCount[t][Community[t][index]] + leaveListCount[t][Community[t][index]])
                print(SI, t, key, index)
#print(dictList[3]['13'])



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


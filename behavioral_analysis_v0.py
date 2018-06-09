"""
Manually fixed some Name not identified:
leroiduch??teaudebrume
Jan1234 (not found) assigned 21
darkchim\xc3\xa4re
z\xc3\x96ghuL
"""

src_path = "dataset/"

import csv
import collections
import pprint
import numpy as np

# assume the clusters are already defined with Ci[]
# C = [[1, 2], [3, 4, 5], [6]]

# the node set N = [1, 2, 3, 4, 5, 6]
# the timestamps T = [1, 2, 3]

T = [1,2,3,4,5,6,7,8,9,10]
Node = [] # all node name set
Cluster = [] # node name clustered by time
Community = {} # dict of nodes with communities assigned
dictList = [] # list of dicts of nodes clustered for each snapshot

with open("graph.csv", "rb") as fl:
    reader = csv.reader(fl, delimiter=',')
    for row in reader:
        Community[row[1]] = row[5]
# print Community

for i in xrange(1, 10): # now only consider from 01 to 09
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
        #print Node
        Cluster.append(n)

def cluster(d): # Clustering the keys of a dict according to its values
    clusters = {}
    for key, val in d.iteritems():
        clusters.setdefault(val, []).append(key)
    return clusters
#print cluster(Community)

for t in xrange(0, 9): # now only consider from 01 to 09
    tempDict = {}
    for index in Cluster[t]:
        # first discovery the communities for each snapshot
        tempDict[index] = Community[index]
    dictList.append(cluster(tempDict))
#print dictList[0]['28']

print "Running Evolutionary Analysis, please wait... \n"

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
joinListCount.append({'28': 0}) # initialize for time 0
leaveCount = 0
leaveIndex = False
leaveList = []
leaveListCount = []
leaveListCount.append({'28': 0}) # initialize for time 0

for t in xrange(1, 9): # snapshots
    # Events involving communities:
    for key in dictList[t]: # clusters in each timestamp
        # Continue
        if key in dictList[t-1] and set(dictList[t][key]) == set(dictList[t-1][key]):
            continueIndex = True
            continueCount += 1
            #print "Continue event is found:"
            #print "The time is %s. The cluster is %s."%(t, key)
            #print dictList[t][key] 
        # kai-Merge
        if key in dictList[t-1] and len(set(dictList[t][key]) & set(dictList[t-1][key])) > kai/100.0*max(len(dictList[t][key]), len(dictList[t-1][key])):
            kMergeIndex = True
            kMergeCount += 1
            #print "k-Merge event is found:"
            #print "The time is %s. The cluster is %s."%(t, key)
            #print dictList[t][key]
        # kai-Split
        if key in dictList[t-1] and len(set(dictList[t][key]) & set(dictList[t-1][key])) < kai/100.0*max(len(dictList[t][key]), len(dictList[t-1][key])):
            kSplitIndex = True
            kSplitCount += 1
            #print "k-Split event is found:"
            #print "The time is %s. The cluster is %s."%(t, key)
            #print dictList[t][key]
        # Form
        if not key in dictList[t - 1]:
            formIndex = True
            formCount += 1
            formList.append(key)
            #print "Form event is found:"
            #print "The time is %s. The cluster is %s."%(t, key)
        # Dissolve
        #if key in dictList[t - 1]:
            #dissolveIndex = True
            #dissolveCount += 1
            #dissolveList.append(key)
            #print "Dissolve event is found:"
            #print "The time is %s. The cluster is %s."%(t, key)
    for key in dictList[t - 1]:
        if not key in dictList[t]:
            dissolveIndex = True
            dissolveCount += 1
            dissolveList.append(key)
    
    # Events involving individuals:
    # Appear
    if not set(Cluster[t]) - set(Cluster[t-1]) is None:
        appearIndex = True
        appearCount += len(set(Cluster[t]) - set(Cluster[t-1]))
        #print "Appear event is found:"
        #print "The time is %s. The node list is:"%(t)
        #print set(Cluster[t]) - set(Cluster[t-1])
        #appearList.append(set(Cluster[t]) - set(Cluster[t-1]))
    # Disappear
    if not set(Cluster[t-1]) - set(Cluster[t]) is None:
        disappearIndex = True
        disappearCount += len(set(Cluster[t-1]) - set(Cluster[t]))
        #print "Disappear event is found:"
        #print "The time is %s. The node list is:"%(t)
        #print set(Cluster[t-1]) - set(Cluster[t])
        #disappearList.append(set(Cluster[t-1]) - set(Cluster[t]))
    # Join
    joinNumber = {}
    leaveNumber = {}
    for key in dictList[t]:
        if key in dictList[t-1]:
            joinIndex = True
            joinNumber[key] = len(set(dictList[t][key]) - set(dictList[t-1][key]))
            joinCount += joinNumber[key]
            #print "Join event is found:"
            #print "The time is %s. The node list is:"%(t)
            #print set(dictList[t][key]) - set(dictList[t-1][key])
            #joinList.append(set(dictList[t][key]) - set(dictList[t-1][key]))
        else:
            joinNumber[key] = 0
    # Leave
        if key in dictList[t-1]:
            leaveIndex = True
            leaveNumber[key] = len(set(dictList[t-1][key]) - set(dictList[t][key]))
            leaveCount += leaveNumber[key]
            #print "Leave event is found:"
            #print "The time is %s. The node list is:"%(t)
            #print set(dictList[t-1][key]) - set(dictList[t][key])
            #leaveList.append(set(dictList[t-1][key]) - set(dictList[t][key]))
        else:
            leaveNumber[key] = 0

    joinListCount.append(joinNumber)
    leaveListCount.append(leaveNumber)
                       
# BEHAVIORAL ANALYSIS:
# Stability Index
def stability_index(x, T):
    if T < 1:
        return 0
    SI = 0
    communityID = Community[x]
    for t in xrange(1, T + 1):
        SI += 1.0 * len(dictList[t][communityID]) / (joinListCount[t][communityID] + leaveListCount[t][communityID])
    return SI

print joinListCount
for t in xrange(1, 9):
    for key in dictList[t]:
        for index in dictList[t][key]:
            print index
            print stability_index(index, t)



print "\n************ SUMMARY ************\n"

print "Events involving communities:\n"

if continueIndex == False:
    print "\tContinue event not found!"
else:
    print "\tThe number of Continue event is %s"%continueCount
if kMergeIndex == False:
    print "\tk-Merge event not found!"
else:
    print "\tThe number of k-Merge event is %s"%kMergeCount
if kSplitIndex == False:
    print "\tk-Split event not found!"
else:
    print "\tThe number of k-Split event is %s"%kSplitCount
if formIndex == False:
    print "\tForm event not found!"
else:
    print "\tThe number of Form event is %s"%formCount
    #print "\t", formList
if dissolveIndex == False:
    print "\tDissolve event not found!"
else:
    print "\tThe number of Dissolve event is %s"%dissolveCount
    #print "\t", dissolveList

print "\nEvents involving individuals:\n"

if appearIndex == False:
    print "\tAppear event not found!"
else:
    print "\tThe number of Appear event is %s"%appearCount
if disappearIndex == False:
    print "\tDisappear event not found!"
else:
    print "\tThe number of Disappear event is %s"%disappearCount
if joinIndex == False:
    print "\tJoin event not found!"
else:
    print "\tThe number of Join event is %s"%joinCount
if leaveIndex == False:
    print "\tLeave event not found!"
else:
    print "\tThe number of Leave event is %s"%leaveCount

print "\nBehavioral analysis:\n"


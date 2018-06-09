src_path = "dataset/"
main_file = "2018.csv" #"First Set of Data - Transactions.csv"
import csv
import collections
import pprint

with open(main_file, "rb") as fp:
    root = csv.reader(fp, delimiter=',')
    result = collections.defaultdict(list)

    # ignore the header
    #next(root, None) 

    for row in root:
        year = row[6].split("-")[1]
        result[year].append(row)

print "Result:-"        
#pprint.pprint(result)

for i,j in result.items():
    file_path = "%s2018%s.csv"%(src_path, i)
    with open(file_path, 'wb') as fp:
        writer = csv.writer(fp, delimiter=',')
        writer.writerows(j)
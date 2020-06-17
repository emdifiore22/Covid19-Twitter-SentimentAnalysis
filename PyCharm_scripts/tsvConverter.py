import csv

ids = []

# file downloaded from https://crisisnlp.qcri.org/covid19 read for id retrieval
with open("ids_2020-03-29.tsv") as fd:
    rd = csv.reader(fd, delimiter="\t", quotechar='"')
    for row in rd:
        if(row[0] is not "tweet_id"):
            ids.append(row[0])

# code below writes ids.txt (ideal for hydration)
with open("ids_6.txt", "a") as writer:
    for id in ids:
        writer.write(id+"\n")
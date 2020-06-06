import json
data = []
with open('./input/iorestoacasa_1.txt') as f:
    for line in f:
        js = json.loads(line)
        if(js["lang"] == "it"):
            if('retweeted_status' not in js and 'extended_tweet' in js):
                data.append(json.loads(line))

with open('./input/iorestoacasa_2.txt') as f:
    for line in f:
        js = json.loads(line)
        if (js["lang"] == "it"):
            if ('retweeted_status' not in js and 'extended_tweet' in js):
                data.append(json.loads(line))

with open('./input/iorestoacasa_3.txt') as f:
    for line in f:
        js = json.loads(line)
        if (js["lang"] == "it"):
            if ('retweeted_status' not in js and 'extended_tweet' in js):
                data.append(json.loads(line))

with open('./input/iorestoacasa_4.txt') as f:
    for line in f:
        js = json.loads(line)
        if (js["lang"] == "it"):
            if ('retweeted_status' not in js and 'extended_tweet' in js):
                data.append(json.loads(line))

with open('./output/iorestoacasa_1_original.json', 'w') as json_file:
    json.dump(data, json_file)

print(len(data))
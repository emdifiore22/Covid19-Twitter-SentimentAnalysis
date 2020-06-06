import json
data = []


with open('./input/sentix.txt') as f:
    for line in f:
        word_line = line.split("\t")
        lemma = {
            "lemma": word_line[0],
            "positive_score": word_line[3],
            "negativeScore":word_line[4]
        }
        data.append(lemma)

with open('./output/sentix.json', 'w') as fp:
    json.dump(data, fp)


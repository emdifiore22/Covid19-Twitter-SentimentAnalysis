from twarc import Twarc
import jsonlines

# set the auth with twarc configure command on console
# The object below automatically load the configuration needed.
t = Twarc()

with jsonlines.open("tweets.jsonl", "a") as writer:
    for tweet in t.hydrate(open("ids_2.txt")):
        writer.write(tweet)

    for tweet in t.hydrate(open("ids_3.txt")):
        writer.write(tweet)

    for tweet in t.hydrate(open("ids_4.txt")):
        writer.write(tweet)

    for tweet in t.hydrate(open("ids.txt")):
        writer.write(tweet)








import assignment4 as a4
from elasticsearch import Elasticsearch
import time

es = Elasticsearch()

d1 = {
	"name": "haha",
	"email": "sth@sfu.ca"
}
d2 = {
	"name": "heyhey",
	"email": "hey@sfu.ca"
}
es.index(index="sfu", id = 1, body = d1)
es.index(index="sfu", id = 2, body = d2)
time.sleep(1)
print(es.get(index = "sfu", id = 2))
print("Hello world")

from pymongo import Connection, collection 
from bloomfilter import bloomify

DB_NAME = 'testdb2'
COLL_NAME = 'testcoll2'
connection = Connection()
db = connection[DB_NAME]
coll = db[COLL_NAME]    

#remove everything in the collection
coll.remove()

data = [{"name":"user_a", "value":1},
        {"name":"user_b", "value":2},
        {"name":"user_c", "value":3},
        {"name":"user_d", "value":4}
        ]

#populate collection with documents (data)
coll.insert(data)

#generate bloom filter based on 'name' 
bfcoll = bloomify(coll, "name")

#checks (all elements will be within the set):
for doc in data:
    assert( coll._bf.contains(doc["name"]) )    #will return True
    assert( None != coll.find_one(doc) )        #will not return None

#attempt to find something not in the database
qry = {"name":"XXXy"}
assert( not(coll._bf.contains("XXXy")) ) #SHOULD return False since XXX not in coll
assert( None == coll.find_one(qry) )     #will return None object since XXX not in coll



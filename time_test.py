"""
    Generates data and performs set-membership queries. Will also provide running time statistics.
"""

from pymongo import Connection, collection 
from gendata import gendata
from bloomfilter import BloomFilter, bloomify
import time

DB_NAME = 'testdb'
COLL_NAME = 'testcoll'
connection = Connection()
db = connection[DB_NAME]
coll = db[COLL_NAME]    

#remove everything in the collection
coll.remove()

#populate database with data/biz.dat
for line in open('data/biz.dat'):
    binfo_l = map(lambda s:s.strip(), line.split('\t'))
    assert(len(binfo_l) == 2)
    coll.insert({'name':binfo_l[0], 'url':binfo_l[1]})

#generate bloom filter based on 'url' 
bfcoll = db[COLL_NAME] #get another collection object
bfcoll = bloomify(bfcoll, 'url')

#generate test files
T1 = 'data/testset'; gendata(1000,0.5, output_file=open(T1,'w'))

#function to performtimed tests
def timed_test(testf_path, coll):
    tic = time.clock()
    for line in open(testf_path):
        binfo_l = map(lambda l:l.strip(), line.split("\t"))
        assert(len(binfo_l)==2)
        [bname, burl] = binfo_l
        coll.find_one({'url':burl})
    toc = time.clock()
    return toc-tic


#make sure both coll objects do not contain the same find_one function
assert(not(coll.find_one == bfcoll.find_one)) 

#perform ntrials of tests on each collection object
ntrials = 5.0
TFILE = T1
coll_res = [timed_test(TFILE, coll) for i in range(int(ntrials))]
bfcoll_res = [timed_test(TFILE, bfcoll) for i in range(int(ntrials))]

#print out statistics
avg_coll_time = sum(coll_res) / ntrials
avg_bfcoll_time = sum(bfcoll_res) / ntrials
print "normal_times:", coll_res 
print "filtered_times:", bfcoll_res
print "normal: (%f) \nfiltered: (%f)"%(avg_coll_time, avg_bfcoll_time)
print "bloomfilter miss_rate: ", bfcoll._bf.get_miss_rate()

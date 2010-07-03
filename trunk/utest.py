import unittest
from pymongo import Connection
from bloomfilter import BloomFilter
import settings

DB_NAME = 'testdb' #change to access from settings file
COLL_NAME = 'testcoll'
connection = Connection()
db = connection[DB_NAME]
coll = db[COLL_NAME]    


class TestLoadingData(unittest.TestCase):
    """
    Tests to see if loading of data is sucessful
    """

    def get_test_data(self, datafile='data/biz.dat'):
        """
        Reads in specified datafile and returns a generator
        which will yield [business_name, business_url]
        for each call to next()
        """
        file = open(datafile)
        for line in file:
            binfo_l = map(lambda s: s.strip(), line.split("\t"))
            assert(len(binfo_l)==2)
            yield binfo_l
        file.close()

    def test_load(self):
        """
        Attempt to load everything from data file into db
        """
        coll.remove() #clear out everything in the collection
        for [bname,burl] in self.get_test_data():
            coll.insert({'name':bname, 'url':burl})

    def test_verify(self):
        """
        verifies that everything was loaded properly during test_load
        """    
        for [bname,burl] in self.get_test_data():
            if not(coll.find_one({'name':bname, 'url':burl})):
                self.assertTrue(False) #could ont find the item


class TestBloomFilter(unittest.TestCase):
    """
    Tests basic functionality of bloomfilter
    """ 
           
    def test_simple(self):
        ELM1 = 'something'
        ELM2 = 'something else'
        bf = BloomFilter(100000)
        bf.add_key(ELM1)
        self.assertTrue(bf.contains(ELM1))
        #very insignificant chance that this next assertion will FAIL
        self.assertTrue(not(bf.contains(ELM2)))

    def test_simple2(self):
        ELM1 = 'something'
        ELM2 = 'something else'
        bf = BloomFilter(100000)
        bf.add_key(ELM1)
        bf.add_key(ELM2)
        self.assertTrue(bf.contains(ELM1))
        self.assertTrue(bf.contains(ELM2))

if __name__ == "__main__":
    unittest.main()

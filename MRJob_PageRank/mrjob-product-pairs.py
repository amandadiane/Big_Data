# Template for writing MapReduce programs using mrjob
# % python mrjob-product-pairs.py product_pairs.txt  -q

from mrjob.job import MRJob
from mrjob.step import MRStep

# change the name of the class
class MR_product_pairs(MRJob):

    def mapper(self, _, line):
        line = line.split(',')
        for i in range(1, len(line)):
            for x in range(i+1, len(line)):
                pair = [int(line[i].strip()),int(line[x].strip())]
                pair.sort()
                yield pair, 1

    def reducer(self, pair, counts):  
        values = list(counts)
        yield pair, sum(values)
        

if __name__ == '__main__':
    # change to match the name of the class
    MR_product_pairs.run()
    

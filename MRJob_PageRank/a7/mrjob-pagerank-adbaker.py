# Amanda Baker
# mrjob-pagerank-adbaker.py graph-1.txt --nodes 4 --beta 0.85 -N 10 -q

from mrjob.job import MRJob
from mrjob.step import MRStep

class MR_pagerank(MRJob):

    def configure_args(self):
        super(MR_pagerank, self).configure_args()
        self.add_passthru_arg('--nodes')    
        self.add_passthru_arg('--beta')    
        self.add_passthru_arg('-N')    

    def mapper_1(self, _, line):
        '''Takes an incidence vector representation, and yields each neighbor node,
        the probability of navigating to that neighbor, and the current node's neighbors'''
        num_nodes = int(self.options.nodes)
        vector_dict = {}
        line = line.split()
        y = line[0]
        neighbors_lst = [line[i] for i in range(1,len(line))]
        for n in neighbors_lst:
            yield n, (1/num_nodes)/len(neighbors_lst)
        yield y, neighbors_lst

    def reducer_1(self, x, pr_y_by_n_or_nbrs_of_x):  
        '''Takes all navigation probabilities per node, and calculates the probability of
        landing on the node for a given beta and number of total nodes; returns each
        neighbor node, the probability of navigating to that neighbor, and the current
        node's neighbors'''
        num_nodes = int(self.options.nodes)
        beta = float(self.options.beta)
        values = list(pr_y_by_n_or_nbrs_of_x)
        probs = [i for i in values if type(i) is float]
        neighbors_lst = next(i for i in values if type(i) is list)
        prob_x = ((1-beta)/num_nodes) + (beta * sum(probs))
        print(x, prob_x)
        for n in neighbors_lst:
            yield n, prob_x/len(neighbors_lst) 
        yield x, neighbors_lst

    def reducer_2(self, x, pr_y_by_n_or_nbrs_of_x):  
        '''Takes all navigation probabilities per node, and calculates the final
        probability of landing on that node'''
        num_nodes = int(self.options.nodes)
        beta = float(self.options.beta)
        values = list(pr_y_by_n_or_nbrs_of_x)
        probs = [i for i in values if type(i) is float]
        prob_x = ((1-beta)/num_nodes) + (beta * sum(probs))
        yield x, prob_x

    def steps(self):
        N = int(self.options.N)
        return [MRStep(mapper=self.mapper_1)] + \
               [MRStep(reducer=self.reducer_1)]*N + \
               [MRStep(reducer=self.reducer_2)]
               

if __name__ == '__main__':
    MR_pagerank.run()
    

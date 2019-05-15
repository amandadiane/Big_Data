# Template for writing MapReduce programs using mrjob
# % python mrjob-pagerank.py <input file>  -q

from mrjob.job import MRJob
from mrjob.step import MRStep

class MR_program(MRJob):

    def configure_args(self):
        super(MR_program, self).configure_args()
        self.add_passthru_arg('--nodes')    ##  <--- specify which new command line args are used
        self.add_passthru_arg('--beta')    ##  <--- specify which new command line args are used
        self.add_passthru_arg('-N')    ##  <--- specify which new command line args are used

    def mapper_1(self, _, line):
        num_nodes = int(self.options.nodes)
        # TODO

    def reducer_1(self, x, pr_y_by_n_or_nbrs_of_x):  
        num_nodes = int(self.options.nodes)
        beta = float(self.options.beta)
        # TODO

    def reducer_2(self, x, pr_y_by_n_or_nbrs_of_x):  
        num_nodes = int(self.options.nodes)
        beta = float(self.options.beta)
        # TODO

    def steps(self):
        N = int(self.options.N)
        return [MRStep(mapper=self.mapper_1)] + \
               [MRStep(reducer=self.reducer_1)]*N + \
               [MRStep(reducer=self.reducer_2)]
               

if __name__ == '__main__':
    # change to match the name of the class
    MR_program.run()
    

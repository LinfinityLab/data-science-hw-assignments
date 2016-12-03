import numpy as np

from mrjob.job import MRJob
from itertools import combinations, permutations
import operator
from scipy.stats.stats import pearsonr


class RestaurantSimilarities(MRJob):

    def steps(self):
        "the steps in the map-reduce process"
        thesteps = [
            self.mr(mapper=self.line_mapper, reducer=self.users_items_collector),
            self.mr(mapper=self.pair_items_mapper, reducer=self.calc_sim_collector)
        ]
        return thesteps

    def line_mapper(self,_,line):
        "this is the complete implementation"
        user_id,business_id,stars,business_avg,user_avg=line.split(',')
        yield user_id, (business_id,stars,business_avg,user_avg)

    def users_items_collector(self, user_id, values):
        """
        #iterate over the list of tuples yielded in the previous mapper
        #and append them to an array of rating information
        """
        rating_information = []
        for data in values:
            rating_information.append(data)
        yield (user_id, rating_information)
        
    def pair_items_mapper(self, user_id, values):
        """
        ignoring the user_id key, take all combinations of business pairs
        and yield as key the pair id, and as value the pair rating information
        """
        #your code here
        my_rest = []
        for r in values:
            my_rest.append(r[0])
        for rest,val in zip(combinations(my_rest,2),combinations(values,2)):
            yield rest,val

    def calc_sim_collector(self, key, values):
        """
        Pick up the information from the previous yield as shown. Compute
        the pearson correlation and yield the final information as in the
        last line here.
        """
        #your code here
        (rest1, rest2), common_ratings = key, values      
        n_common = 0
        diff1 = []
        diff2 = []

        for v in common_ratings:
            diff1.append(np.float(v[0][1]) - np.float(v[0][3]))
            diff2.append(np.float(v[1][1]) - np.float(v[1][3]))
            n_common += 1

        rho = pearsonr(diff1, diff2)[0]
        if np.isnan(rho):
                rho = 0
        yield (rest1, rest2), (rho, n_common)

#Below MUST be there for things to work
if __name__ == '__main__':
    RestaurantSimilarities.run()
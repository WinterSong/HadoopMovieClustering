from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
import numpy as np
from scipy import spatial
import math

class MovieClustering(MRJob):
    def configure_options(self):
        super(MovieClustering,self).configure_options()
        self.dist = {}
        self.centers = []
        self.whoisyourcenter = [-1]*20000
        self.add_file_option('--files',dest="file_loc",help='path to movies.dat')

        self.add_passthrough_option(
            '--t1', dest = 'T1',
            type='float', default=0.80,
            help='specifying the T1 threshold used in canopy'
        )
        self.add_passthrough_option(
            '--t2', dest = 'T2',
            type='float', default=0.95,
            help='specifying the T2 threshold used in canopy'
        )
        self.add_passthrough_option(
            '--iter', dest = 'iterations',
            type='int', default=10,
            help='specifying the iteration times for k-means algorithm'
        )
        self.add_passthrough_option(

            '--mNum', dest = 'movieNumber',
            type = 'int', default = 17770,
            help='number of movies'
        )

    def steps(self):
        return [
            MRStep( mapper = self.mapper_preprocess,
                    reducer = self.reducer_preprocess),
            MRStep( mapper = self.mapper_formpairs,
                    reducer = self.reducer_combine_pairs),
            MRStep( mapper=self.mapper_result,
                    reducer = self.reducer_result,
                    reducer_final = self.canopy_mapper)]+\
            [MRStep( mapper = self.mapper_kmeans_selection,
                    reducer = self.reducer_kmeans_selection)]*\
                    self.options.iterations+\
            [MRStep( mapper = self.mapper_kmeans_selection,
                     reducer = self.displayResultReducer)]

    def mapper_preprocess(self, key, line):
        movie, user, rating, _ = line.split(',')
        yield user, (movie, int(rating))


    def reducer_preprocess(self, userid, mappair):
        ratings = list()
        for movieid, rating in mappair:
            ratings.append((movieid, rating))
        yield userid, ratings

    def mapper_formpairs(self, uid, ratings):
        for val1, val2 in combinations(ratings,2):
            m1 = val1[0]
            r1 = val1[1]
            m2 = val2[0]
            r2 = val2[1]
            yield (m1,m2), (r1,r2)

    def reducer_combine_pairs(self, mpair, rpair):
        ratingsList = []
        for rate in rpair:
            ratingsList.append(rate)
        yield mpair, ratingsList

    def mapper_result(self, mpair, ratingsList):
        mSum = 0
        A2 = 0
        B2 = 0
        for rate in ratingsList:
            mSum += rate[0]*rate[1]
            A2 += rate[0]**2
            B2 += rate[1]**2
        yield mpair, mSum/(A2*B2)**0.5

    def reducer_result(self, mpair, score):
        with open('score.dat','a') as f:
            f.write(mpair[0]+','+mpair[1]+'\t'+str([s for s in score][0])+'\n')

    def init_dist(self):
        with open('score.dat','r') as f:
            for line in f.readlines():
                arr = line.strip().split('\t')
                mpair = arr[0].split(',')
                score = float(arr[1])
                m1 = int(mpair[0])
                m2 = int(mpair[1])
                if m1 in self.dist:
                    self.dist[m1][m2] = score
                else:
                    self.dist[m1] = {}
                    self.dist[m1][m2] = score
                if m2 in self.dist:
                    self.dist[m2][m1] = score
                else:
                    self.dist[m2] = {}
                    self.dist[m2][m1] = score

    def canopy_mapper(self):
        self.init_dist()
        rawSet = range(1,self.options.movieNumber+1)
        while len(rawSet)>0:
            deleteSet = []
            current = rawSet.pop(0)
            self.centers.append(current)
            self.whoisyourcenter[current] = current
            for idx, point in enumerate(rawSet):
                if current not in self.dist[point]:
                    continue
                if self.dist[point][current] > self.options.T1:
                    self.whoisyourcenter[point] = current
                if self.dist[point][current] > self.options.T2:
                    deleteSet.append(idx)
            for idx, rdx in enumerate(deleteSet):
                rawSet.pop(rdx-idx)
        for i in range(1,self.options.movieNumber+1):
            yield i, (self.whoisyourcenter[i], self.dist[i])

    def mapper_kmeans_selection(self, mid, center_distMat):
        center = center_distMat[0]
        distMat = center_distMat[1]
        yield center, (mid, distMat)

    def reducer_kmeans_selection(self, center, mid_distMat):
        maxTotalSimilarity = 1
        newCenter = center
        distMat = {}
        mid = []
        for instance in mid_distMat:
            mid.append(instance[0])
            if instance[0] not in distMat:
                distMat[instance[0]] = {}
            for ins in instance[1]:
                distMat[instance[0]][int(ins)] = instance[1][ins]
        for m in mid:
            if m == center:
                continue
            maxTotalSimilarity *= distMat[m][center]
        for candidate in mid:
            TotalSimilarity = 1
            for m in mid:
                if m == candidate:
                    continue
                if candidate in distMat[m]:
                    TotalSimilarity *= distMat[m][candidate]
                else:
                    TotalSimilarity *= 0.8
            if TotalSimilarity > maxTotalSimilarity:
                maxTotalSimilarity = TotalSimilarity
                newCenter = candidate
        for m in mid:
            yield m, (newCenter, distMat[m])

    def displayResultReducer(self, center, mid_distMat):
        for instance in mid_distMat:
            print center, instance[0], instance[1]

if __name__ == '__main__':
    MovieClustering.run()

#######################################
# 
# Spring 2014
# MRJob K-means Implementation
#
# Diego Rodriguez
#
#######################################

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os.path
import numpy as np

WORD_RE = re.compile(r"[\w']+")

class KMeans1Iteration(MRJob):
    
    #Initialize cluster and data structures with zeros
    #WARNING: Dummy initial row (Ignore in code)... documented so ok =)
    clusters = np.zeros(shape=(1,3))  #This is a numpy array
    #data is x, y, number in centroid
    data = {}   #This is a dictionary (hash function)
    k = 0
    c = 0  #keep track of change in centroid in reducer
    first = True
    count = 0
    sum_x = 0
    sum_y = 0
    
    #File option: The file specified will be available to each task local directory
    #and the value of the option will magically be changed to its path
    def configure_options(self):
        super(KMeans1Iteration, self).configure_options()
        self.add_file_option('--centroids')

    def steps(self):
        return [
            MRStep(mapper_init=self.init_get_centroids,
                   mapper=self.mapper_assign_points,
                   mapper_final=self.final_compute_centroids,
                   reducer=self.reducer_combineCentroids)
        ]

    #Using mapper_init to load centroids file
    def init_get_centroids(self):
        # Read file (after checking that it exists)
        #Columns: X, Y, CENTROID_ID
        if not os.path.exists(self.options.centroids):
            #file(self.options.centroids, 'w').close()
            print 'Centroid File Not Fount'
        else: 
            f = open(self.options.centroids, 'r')
            for fileline in f:
                c = fileline.split(',')
                self.clusters = np.append(self.clusters, [[float(c[0].strip('\n')), float(c[1].strip('\n')), float(c[2].strip('\n'))]], axis=0)
            f.closed
            print self.clusters
            
        #Set k
        print self.clusters.shape[0]
        self.k = self.clusters.shape[0] - 1
        print self.k
        
        #Initialize data dictionary
        for i in range (1, self.k + 1):
            #data is x, y, number in centroid
            self.data[i] = [0,0,0]
        

    def mapper_assign_points(self, _, line):     #ASSIGN LABELS TO EACH DATA POINT AND RECORD RUNNING TOTAL BY CENTROID
        #DEBUG: Verify clusters available to mappers (comment out)
        #print self.clusters
        
        #Import Iris dataset
        c = line.split(',')
        tarray = np.array([float(c[0]), float(c[1])])
        
        #Assign points to closest centroid
        maxDist = 999
        cAssign = 0
        
        for i in range (1, self.k + 1):
            if(np.sqrt(sum((self.clusters[i,0:2] - tarray) ** 2)) < maxDist):
                maxDist = np.sqrt(sum((self.clusters[i,0:2] - tarray) ** 2))
                cAssign = i
        
        #Add points to data array (actually assign running total and number of points assigned to centroid)
        #while assigning it to closet centroid
        sum_x = self.data[cAssign][0]
        sum_y = self.data[cAssign][1]
        centroid_count = self.data[cAssign][2]
        self.data[cAssign] = [sum_x + float(c[0]), sum_y + float(c[1]), centroid_count + 1]

    
    def final_compute_centroids(self):    #UPDATE CLUSTER CENTROIDS
        #DEBUG: Verify data available to mapper_final (comment out)
        #print self.k
        #print self.clusters
        #print self.clusters.shape
        #print self.data
        
        for i in range (1, self.k + 1):
            #Calculate mean
            sum_x = self.data[i][0]
            sum_y = self.data[i][1]
            centroid_count = self.data[i][2]
        
            #Update Cluster (POTENTIAL IMPROVEMENT: Random Restart... to implement later)
            #key=cluster id, value=x-y
            #print i, [tarray[0,0],tarray[0,1]]
            yield i, [sum_x/centroid_count,sum_y/centroid_count]
        
        #yield dummy record to trigger reducer 
        yield 999, [0.0,0.0]
            
    def reducer_combineCentroids(self, centroid_id, centroid_coor):
        l = list(centroid_coor)[0]    #get list
        x = l[0]  #get x coordinate
        y = l[1]  #get y coordinate
        
        print centroid_id, x, y
        
        if(centroid_id != self.c):    #New centroid records start (centroid labelled 1,2,3)
            #New record so Yield results (except if first record)
            if(self.first == False):
                yield 1, str(self.sum_x/self.count) + ',' + str(self.sum_y/self.count) + ',' + str(self.c)
            
            self.c = centroid_id
            self.sum_x = x
            self.sum_y = y
            self.count = 1.0
            self.first = False
        else:    #subsequent centroid record
            self.sum_x = self.sum_x + x
            self.sum_y = self.sum_y + y
            self.count = self.count + 1.0

        
if __name__ == '__main__':
    KMeans1Iteration.run()
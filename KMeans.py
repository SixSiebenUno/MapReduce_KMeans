#######################################
# 
# Spring 2014
# MRJob K-means Implementation
#
# Diego Rodriguez
#
#######################################

from time import strftime, time, localtime
from KMeans1Iteration import KMeans1Iteration
import numpy as np

def runJob(MRJobClass, argsArr, loc='local', cwd='not_set'):
    
    if loc == 'emc':
        argsArr.extend(['-r', 'emc'])
    
    #Extending arguments to include file option
    #The file will be available to all tasks (see class for details)
    #NOTE: This program assume a centroid file is available (initialization of centroids not covered here)
    centroidfile = cwd + 'centroid.txt' 
    argsArr.extend(['--centroids=%s' %centroidfile])
    
    print "path set to: %s" %cwd
    print "centroid file: %s" %centroidfile
    print "argsArr: "
    print argsArr
    
    startTime=time()
    print "%s starting %s job on %s" % (strftime("%a, %d %b %Y %H:%M::%S", localtime()), MRJobClass.__name__, loc)
    mrJob = MRJobClass(args=argsArr)
    runner = mrJob.make_runner()
    runner.run()
    
    #Dump output to file (centroid file... so next iteration knows what the latest centroids were)
    f = open(centroidfile, 'w')
    for line in runner.stream_output():
        key, value = mrJob.parse_output_line(line)
        print ('From Hadoop: ', key, value)
        f.write(value + '\n')
    f.closed
    
    endTime=time()
    jobLength=endTime-startTime
    print "%s finished %s job in %d seconds" % (strftime("%a, %d %b %Y %H:%M::%S", localtime()), MRJobClass.__name__, jobLength)
    return mrJob, runner

#Set path and data file name:
#Could pass as parameters, but to lazy to retype that stuff on the command line =)
cwd = "/Users/diego/Dropbox/_Diego/_Proyectos/Eclipse/MRJob_KMeans2/"
data = "iris.data.txt"
centroidfile2 = cwd + 'centroid.txt'
runtype = "local"
tol = 0.01
n = 100
datafile = cwd + data
print "datafile: %s" %datafile
for i in range (n):
    centroids_new = np.loadtxt(centroidfile2,delimiter=",")
    runJob(KMeans1Iteration, ['%siris.data.txt' %cwd], runtype, cwd)
    centroids_old = np.loadtxt(centroidfile2,delimiter=",")
    mx = np.max(abs(centroids_new-centroids_old))
    print ('runJob No. %d' % i)
    if mx < tol:
        print ('tolerance reached')
        break







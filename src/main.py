import argparse
import os
import sys
import time
import importlib

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')


try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark

from pyspark.sql import SparkSession

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name', help="The name of the job module you want to run. (ex: poc will run job on jobs.poc package)")
    parser.add_argument('--job-args', nargs='*', help="Extra arguments to send to the PySpark job (example: --job-args template=manual-email1 foo=bar")

    args = parser.parse_args()
    print "Called with arguments: %s" % args

    job_args = dict()
    if args.job_args:
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]
        print 'job_args_tuples: %s' % job_args_tuples
        job_args = {a[0]: a[1] for a in job_args_tuples}

    #print '\nRunning job %s...\nenvironment is %s\n' % (args.job_name, environment)
    
    spark = SparkSession.builder.appName(args.job_name).getOrCreate()
    df = spark \
        .read \
        .format("csv") \
        .option("header", "true") \
        .load("data/train.csv")

    job_module = importlib.import_module('jobs.%s' % args.job_name)

    start = time.time()
    res = job_module.analyze(spark,df,**job_args)
    print(res)
    end = time.time()

    #print "\nExecution of job %s took %s seconds" % (args.job_name, end-start)

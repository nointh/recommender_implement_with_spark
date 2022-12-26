
import numpy as np
from numpy.linalg import inv
import datetime
from pyspark.sql import SparkSession
import argparse
import math
import json

def main(params):
    model = params.model
    output = params.output
    input = params.input
    stepsize = params.stepsize
    maxiter = params.maxiter
    lambd = params.lambd
    numfactor = params.numfactor

    spark = SparkSession\
        .builder\
        .getOrCreate()
    sc = spark.sparkContext
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='argument for spark jobs')

    parser.add_argument('--input', default='gs://movie_recommenders/20221218T143829/processed/ratings.csv')
    parser.add_argument('--output', default='gs://movie_recommenders/20221218T143829/model/')
    parser.add_argument('--model', default='sgd')
    parser.add_argument('--stepsize', type=float, default=0.01)
    parser.add_argument('--maxiter', type=int, default=0)
    parser.add_argument('--lambd', type=float, default=1)
    parser.add_argument('--numfactor', type=float, default=10)

    args = parser.parse_args()
    main(args)
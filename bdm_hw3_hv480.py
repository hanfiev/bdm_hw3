from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

import sys

def main(sc):
  import csv
  import json
  import numpy as np
  import pandas as pd

  spark = SparkSession(sc)

  kf_rdd = sc.textFile('keyfood_products.csv', use_unicode=True).cache()
  reader = csv.reader(open('keyfood_sample_items.csv', 'r'))
  sample_dict = {'upc_code':[],'item_name':[]}
  for row in reader:
    upc_code, item_name = row
    sample_dict['upc_code'].append(upc_code[4:])
    sample_dict['item_name'].append(item_name)

  sample_dict['item_name'] = sample_dict['item_name'][1:]
  sample_dict['upc_code'] = sample_dict['upc_code'][1:]

  foodInsecurityDict = {}

  # Opening JSON file
  f = open('keyfood_nyc_stores.json')
  
  # returns JSON object as
  # a dictionary
  data = json.load(f)

  f.close()

  #create new dictionary with the store name as the key, and food insecurity as the value
  for key in data:
    foodInsecurityDict[key] = round(data[key]['foodInsecurity']*100)

  outputTask1 = kf_rdd.filter(lambda x: not x.startswith('store') and \
                      '-' in x.split(',')[2] and \
                      x.split(',')[2].split('-')[1] in sample_dict['upc_code']) \
                      .map(lambda x: x.split(',')) \
                      .map(lambda x: (x[3], float(x[5].split('\xa0')[0][1:]), int(str(x[0]).replace(str(x[0]),str(foodInsecurityDict[str(x[0])])))))

  outputTask1 = outputTask1.cache()
  outputTask1.count()

  outputTask1.saveAsTextFile(sys.argv[1] if len(sys.argv)>1 else 'output')


if __name__=="__main__":
  sc = SparkContext()
  main(sc)


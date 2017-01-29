# Yelp Data Parser

This repository contains a [docker](https://www.docker.io/) image to run [Apache Spark](https://spark.apache.org/) client along with the python script to parse [yelp data](https://www.yelp.com/dataset_challenge/dataset)

## Script Info
The python script <b>coding_test.py</b> processes yelp dataset provided in the following ways:
1. in form of single json file
2. in form of directory containing json files
3. in form of compressed tar.gz file containing one or more json files

Script accepts 2 parameters <b>-f</b> containing path to the dataset location and <b>-q</b> containing query i.e. comma-separated types of dataset namely (user,business,reviews, tip, checkin)


The query outputs the following info about above mentioned types:
1. user: lists all users with more than 4 star rating
2. reviews: Number of positive, negative and neutral reviews. Criteria based on keyword:
    - positive: stars
    - negative: bad
    - neutral: all others
3. business: lists all business states by their average review count
4. checkin: show total number of check-ins
5. tip: Show number of tips per year

## Run in PySpark environment

###To query single dataset:
```
pyspark coding_test.py -f "/path/to/dataset.tar.gz" -q user
``` 

###To query multiple datasets:
```
pyspark coding_test.py -f /path/to/dataset.tar.gz -q user,business,tip,checkin,reviews
```

## Run in docker environment
Execute the following scripts in the path containing the clone of this git repository.

###To build image
```
docker build -t docker-newyorker-test .
```

###To run the script in docker image
```
docker run -it -p 4040:4040 -v $(pwd)/coding_test.py:/coding_test.py -v $(pwd)/yelp_dataset.tar.gz:/yelp_dataset.tar.gz docker-newyorker-test /spark/bin/spark-submit /coding_test.py -f /yelp_dataset.tar.gz -q user,tip,business,reviews,checkin
```


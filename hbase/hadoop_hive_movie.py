# -*- coding: utf-8 -*-
"""
Created on Mon Feb 25 14:51:31 2019

@author: Ryan
"""
from starbase import Connection

c = Connection('127.0.0.1', '8000')

ratings = c.table('ratings')

if(ratings.exists()):
    print("drop existed table\n")
    ratings.drop()
    
ratings.create('rating')

print('parse the ml-100k ratings data ...\n')
ratingFile = open("E:/datasets/ml-100k/u.data", "r")

batch = ratings.batch()

for line in ratingFile:
    (userID, movieID, rating, timestamp)= line.split()
    batch.update(userID, {'rating':{movieID: rating}})
    
ratingFile.close()

print("commit data to hbase via rest service\n")
batch.commit(finalize=True)

print("Get ratings of specific user\n")
print("for the user 1:")
print(ratings.fetch(1))
print("for the user 33:" )
print(ratings.fetch(33))

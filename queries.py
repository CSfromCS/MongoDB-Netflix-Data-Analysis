# Emmanuel Linus T. Evangelista, 201996
# Karl Robyn Gue, 202438
# Radyn Punla, XXXXXX
# Christian Sarabia, 204624
# December 4, 2023

# We certify that this submission complies with the DISCS Academic Integrity
# Policy.

# If we have discussed our Python language code with anyone other than
# our instructor(s), our groupmate(s), the teaching assistant(s),
# the extent of each discussion has been clearly noted along with a proper
# citation in the comments of our program.

# If any Python language code or documentation used in our program
# was obtained from another source, either modified or unmodified, such as a
# textbook, website, or another individual, the extent of its use has been
# clearly noted along with a proper citation in the comments of our program.

################################################################################

import pymongo
from pymongo import MongoClient
from pprint import pprint

client = MongoClient('127.0.0.1', 27017)
db = client['final']
coll = db['netflix']

def print_cursor(cursor, num=None):
  if num:
    print('Query #' + num)
  
  for doc in cursor:
    pprint(doc)

def run_queries():
  # 1
  # Number of movies compared to TV shows
  q1 = coll.aggregate([
    {
      '$group': {
        '_id': '$type',
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'type': '$_id',
        'count': 1
      }
    }
  ])
  print_cursor(q1, '1')
  
  # 2a
  # Directors with the highest numbers of movies and TV shows directed
  q2a = coll.aggregate([
    {
      '$unwind': '$director'
    }, {
      '$group': {
        '_id': '$director',
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'name': '$_id',
        'count': 1
      }
    }, {
      '$sort': { 'count': -1 }
    }, {
      '$limit': 10
    }
  ])
  print_cursor(q2a, '2a')

  # 2b
  # Directors who have directed in the most number of categories
  q2b = coll.aggregate([
    {
      '$unwind': '$director'
    }, {
      '$unwind': '$listed_in'
    }, {
      '$group': {
        '_id': {
          'name': '$director',
          'category': '$listed_in'
        },
        'count': { '$sum': 1 }
      }
    }, {
      '$group': {
        '_id': '$_id.name',
        'num_categories': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'director': '$_id',
        'num_categories': 1
      }
    }, {
      '$sort': {
        'num_categories': -1,
        'director': 1
      }
    }, {
      '$limit': 10
    }
  ])
  print_cursor(q2b, '2b')


if __name__ == '__main__':
  run_queries()
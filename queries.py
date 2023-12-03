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

def print_cursor(cursor):
  for doc in cursor:
    pprint(doc)

def run_queries():
  # 1
  # Number of movies compared to TV shows
  q1 = coll.aggregate([
    {
      '$group': {
        '_id': '$type',
        'count': {
          '$sum': 1
        }
      }
    }
  ])
  
  print_cursor(q1)

if __name__ == '__main__':
  run_queries()
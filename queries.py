# Emmanuel Linus T. Evangelista, 201996
# Karl Robyn Gue, 202438
# Radyn Matthew H. Punla, 194000
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
  
  # 3a
  # Countries with the most number of movies
  q3a = coll.aggregate([
    {
      '$match': { 'type': 'Movie' }
    }, {
      '$group': {
        '_id': '$country',
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'country': '$_id',
        'count': 1
      }
    }, {
      '$sort': { 'count': -1 }
    }, {
      '$limit': 3
    }
  ])
  print_cursor(q3a, '3a')
  
  # 3b
  # Countries with the most number of tv shows
  q3b = coll.aggregate([
    {
      '$match': { 'type': 'TV Show' }
    }, {
      '$group': {
        '_id': '$country',
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'country': '$_id',
        'count': 1
      }
    }, {
      '$sort': { 'count': -1 }
    }, {
      '$limit': 3
    }
  ])
  print_cursor(q3b, '3b')
  
  # 4
  # Popular genres for movies and TV shows
  q4amovies = coll.aggregate([
    {
      '$unwind': '$listed_in'
    }, {
      '$match': { 'type': 'Movie' }
    }, {
      '$group': {
        '_id': {
          'category': '$listed_in',
          'type': '$type'
        },
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'category': '$_id.category',
        'type': '$_id.type',
        'count': 1
      }
    }, {
      '$sort': { 'count': -1 }
    }, {
      '$limit': 5
    }
  ])
  q4ashows = coll.aggregate([
    {
      '$unwind': '$listed_in'
    }, {
      '$match': { 'type': 'TV Show' }
    }, {
      '$group': {
        '_id': {
          'category': '$listed_in',
          'type': '$type'
        },
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'category': '$_id.category',
        'type': '$_id.type',
        'count': 1
      }
    }, {
      '$sort': { 'count': -1 }
    }, {
      '$limit': 5
    }
  ])
  print_cursor(q4amovies, '4a: movies')
  print_cursor(q4ashows, '4a: shows')
  
  # 4b
  # Most popular category per country
  # https://www.mongodb.com/community/forums/t/selecting-documents-with-largest-value-of-a-field/107032
  q4b = coll.aggregate([
    {
      '$unwind': '$listed_in'
    }, {
      '$group': {
        '_id': {
          'category': '$listed_in',
          'country': '$country'
        },
        'count': { '$sum': 1 },
      }
    }, {
      '$sort': {
        'country': 1,
        'count': -1,
      }
    }, {
      '$group': {
        '_id': '$_id.country',
        'max_count': { '$max': '$count' },
        'max_val': { '$first': '$$ROOT' }
      }
    }, {
      '$project': {
        '_id': 0,
        'country': '$max_val._id.country',
        'category': '$max_val._id.category',
        'count': '$max_count'
      }
    }, {
      '$limit': 10
    }
  ])
  print_cursor(q4b, '4b')
  
  # 5a
  # Ratings with number of shows
  q5a = coll.aggregate([
    {
      '$group': {
        '_id': '$rating',
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'rating': '$_id',
        'count': 1
      }
    }, {
      '$sort': { 'count': -1 }
    }
  ])
  print_cursor(q5a, '5a')
  
  # 5b
  # Correlation between rating and category
  q5b = coll.aggregate([
    {
      '$unwind': '$listed_in'
    }, {
      '$group': {
        '_id': {
          'category': '$listed_in',
          'rating': '$rating'
        },
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'category': '$_id.category',
        'rating': '$_id.rating',
        'count': 1
      }
    }, {
      '$sort': { 'count': -1 }
    }, {
      '$limit': 10
    }
  ])
  print_cursor(q5b, '5b')
  
  # 6a
  # Minimum, maximum, and average duration of movies in minutes
  q6a = coll.aggregate([
    {
      '$match': { 'type': 'Movie' }
    }, {
      '$group': {
        '_id': 0,
        'min_duration': { '$min': '$movie_min' },
        'max_duration': { '$max': '$movie_min' },
        'ave_duration': { '$avg': '$movie_min' },
      }
    }, {
      '$project': {
        '_id': 0
      }
    }
  ])
  print_cursor(q6a, '6a')
  
  # 6b
  # Minimum, maximum, and average number of seasons of TV shows
  q6b = coll.aggregate([
    {
      '$match': { 'type': 'TV Show' }
    }, {
      '$group': {
        '_id': 0,
        'min_duration': { '$min': '$tv_seasons' },
        'max_duration': { '$max': '$tv_seasons' },
        'ave_duration': { '$avg': '$tv_seasons' }
      }
    }, {
      '$project': {
        '_id': 0
      }
    }
  ])
  print_cursor(q6b, '6b')
  
  # 7a
  # Most common month and year added to Netflix
  q7a = coll.aggregate([
    {
      '$group': {
        '_id': {
          'year': '$year_added',
          'month': '$month_added'
        },
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'month': '$_id.month',
        'year': '$_id.year',
        'count': 1
      }
    }, {
      '$sort': { 'count': -1 }
    }, {
      '$limit': 10
    }
  ])
  print_cursor(q7a, '7a')
  
  # 7b
  # Most common release day of the week
  q7b = coll.aggregate([
    {
      '$group': {
        '_id': '$day_of_week_added',
        'count': { '$sum': 1 }
      }
    }, {
      '$sort': { 'count': -1 }
    }
  ])
  print_cursor(q7b, '7b')
  
  # 7c
  # Movies that were added late
  # https://stackoverflow.com/questions/33891511/mongodb-concat-int-and-string
  q7c = coll.aggregate([
    {
      '$project': {
        '_id': 0,
        'title': 1,
        'release_year': 1,
        'date_added': {
          '$concat': [
            { '$substr': ['$month_added', 0, -1] },
            '/',
            { '$substr': ['$day_added', 0, -1] },
            '/',
            { '$substr': ['$year_added', 0, -1] },
          ],
        },
        'year_delay': { '$subtract': ['$year_added', '$release_year'] }
      }
    }, {
      '$sort': { 'year_delay': -1 }
    }, {
      '$limit': 5
    }
  ])
  print_cursor(q7c, '7c')

if __name__ == '__main__':
  run_queries()
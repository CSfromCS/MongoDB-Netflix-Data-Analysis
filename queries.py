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
    print('\nQuery #' + num)
  
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
  
  # 2c
  # Most common first name for directors
  # https://stackoverflow.com/questions/21387969/mongodb-count-the-number-of-items-in-an-array
  # https://stackoverflow.com/questions/38377582/mongodb-aggregation-join-array-of-strings-to-single-string
  q2c = coll.aggregate([
    {
      '$unwind': '$director'
    }, {
      '$match': {
        'director': {
          '$not': { '$eq': 'Not Given' }
        }
      }
    }, {
      '$group': {
        '_id': '$director'
      }
    }, {
      '$project': {
        '_id': 0,
        'director': '$_id',
        'director_split': {
          '$split': ['$_id', ' ']
        }
      }
    }, {
      '$project': {
        'director': 1,
        'director_firstname': { '$first': '$director_split' },
        'director_lastname': {
          '$reduce': {
            'input': {
              '$slice': [
                '$director_split',
                { '$subtract': [1, { '$size': '$director_split' }] }
              ],
            },
            'initialValue': '',
            'in': {
              '$concat': [
                '$$value',
                { '$cond': [{'$eq': ['$$value', '']}, '', ' '] },
                '$$this'
              ]
            }
          }
        }
      }
    }, {
      '$group': {
        '_id': '$director_firstname',
        'director_lastnames': {
          '$push': '$director_lastname'
        },
        'count': { '$sum': 1 }
      }
    }, {
      '$sort': { 'count': -1 }
    }, {
      '$limit': 5
    }
  ])
  
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
  
  # 6c
  # Evolution of duration over the years
  q6c = coll.aggregate([
    {
      '$match': { 'type': 'Movie' }
    }, {
      '$group': {
        '_id': '$release_year',
        'max_duration': { '$max': '$movie_min' },
        'ave_duration': { '$avg': '$movie_min' },
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'year': '$_id',
        'max_duration': 1,
        'ave_duration': 1,
        'count': 1
      }
    }, {
      '$sort': { 'year': 1 }
    }
  ])
  
  # 6d
  q6d = coll.aggregate([
    {
      '$match': { 'type': 'TV Show' }
    }, {
      '$group': {
        '_id': '$release_year',
        'max_seasons': { '$max': '$tv_seasons' },
        'ave_seasons': { '$avg': '$tv_seasons' },
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'year': '$_id',
        'max_seasons': 1,
        'ave_seasons': 1,
        'count': 1
      }
    }, {
      '$sort': { 'year': 1 }
    }
  ])
  
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
  
  # 8a
  # Most popular genre in the Philippines
  q8a = coll.aggregate([
    {
      '$match': { 'country': 'Philippines' }
    }, {
      '$unwind': '$listed_in'
    }, {
      '$group': {
        '_id': '$listed_in',
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'category': '$_id',
        'count': 1
      }
    }, {
      '$sort': { 'count': -1 }
    }, {
      '$limit': 5
    }
  ])
  
  # 8b
  # Directors who have directed Filipino movies and shows
  q8b = coll.aggregate([
    {
      '$match': { 'country': 'Philippines' }
    }, {
      '$unwind': '$director'
    }, {
      '$group': {
        '_id': '$director',
        'count': { '$sum': 1 }
      }
    }, {
      '$project': {
        '_id': 0,
        'director': '$_id',
        'count': 1
      }
    }, {
      '$sort': { 'count': -1 }
    }, {
      '$limit': 10
    }
  ])

  # print cursors

  print_cursor(q1, '1')
  print_cursor(q2a, '2a')
  print_cursor(q2b, '2b')
  print_cursor(q2c, '2c')
  print_cursor(q3a, '3a')
  print_cursor(q3b, '3b')
  print_cursor(q4amovies, '4a: movies')
  print_cursor(q4ashows, '4a: shows')
  print_cursor(q4b, '4b')
  print_cursor(q5a, '5a')
  print_cursor(q5b, '5b')
  print_cursor(q6a, '6a')
  print_cursor(q6b, '6b')
  print_cursor(q6c, '6c')
  print_cursor(q6d, '6d')
  print_cursor(q7a, '7a')
  print_cursor(q7b, '7b')
  print_cursor(q7c, '7c')
  print_cursor(q8a, '8a')
  print_cursor(q8b, '8b')

if __name__ == '__main__':
  run_queries()
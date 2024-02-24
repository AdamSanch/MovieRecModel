import sqlite3
import csv
from tqdm import tqdm
import pandas as pd
import numpy as np
from multiprocessing import Pool
import time
import os

"""
------------------------------------------------------------------------------------------------------------------------
 The following functions create and populate the database tables 'movies' and 'ratings'
 The function update_moviesT_parrallel() adds data from the IMDB dataset to the 'movies' table in parallel processes
 
 The database, movie_recommender.db, is created in the data/ directory.

 
 imdb data can be found at https://datasets.imdbws.com/
 ml-latest-small data can be found at https://grouplens.org/datasets/movielens/
 required pip installs: tqdm, pandas, numpy
------------------------------------------------------------------------------------------------------------------------
"""


rating_file = 'data/ml-latest-small/ratings.csv'
movie_file = 'data/ml-latest-small/movies.csv'
link_file = 'data/ml-latest-small/links.csv'

imdb_movie_file = 'data/IMDB-data-BIG/Archive/title.basics.tsv'
imdb_rating_file = 'data/IMDB-data-BIG/Archive/title.ratings.tsv'

database_name = 'movie_recommender.db'


def fill_movie_table():
    with sqlite3.connect(database_name) as conn:
        cursor = conn.cursor()

        # Add movie_id, title, and genres to movies table                    
        with open(movie_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            next(reader)
            for row in tqdm(reader):
                movie_id = int(row[0])
                title = row[1]
                genres = row[2]
                cursor.execute("INSERT INTO movies (movie_id, title, genres) VALUES (?, ?, ?)", (movie_id, title, genres))

        # Add IMDB_id to movies table
        with open(link_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            next(reader)
            for row in tqdm(reader):
                movie_id = int(row[0])
                Imdb_id = int(row[1])
                cursor.execute("UPDATE movies SET IMDB_id=? WHERE movie_id=?", (Imdb_id, movie_id))
        
        # # Create index on IMDB_id variable in movies table
        # cursor.execute("CREATE INDEX IF NOT EXISTS idx_movies_imdb_id ON movies (IMDB_id)")
        # print('Created index on IMDB_id in movies table\n')

# Create and populate ratings table using ratings.csv
def fill_ratings_table():
    with sqlite3.connect(database_name) as conn:
        cursor = conn.cursor()
        
        # Add user_id, movie_id, rating, and timestamp to ratings table
        with open(rating_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            next(reader)
            for row in tqdm(reader):
                user_id = int(row[0])
                movie_id = int(row[1])
                rating = float(row[2])
                timestamp = int(row[3])
                cursor.execute("INSERT INTO ratings (user_id, movie_id, rating, timestamp) VALUES (?, ?, ?, ?)", (user_id, movie_id, rating, timestamp))


"""
------------------------------------------------------------------------------------------------------------------------
 Parrallel processing functions to merge move data from IMDB into the movies table
------------------------------------------------------------------------------------------------------------------------
"""

# Reads and removes a row of data from the chunk if the movie does not exist in the 'movies' table
def process_data(chunk):
    results = []
    with sqlite3.connect(database_name) as conn:
        cursor = conn.cursor()
        reader = csv.reader(chunk, delimiter='\t')
        for row in reader:
            # Check if the 'Imdb_id' variable, with the first 2 letters removed and the rest converted to int, exists in the 'movies' table 
            Imdb_id = int(row[0][2:])
            cursor.execute("SELECT 1 FROM movies WHERE IMDB_id=?", (Imdb_id,))
            data = cursor.fetchone()
            # If if data is not None, add the movie to the results list
            if data is not None:
                results.append(row)            
    return results

# Generator function to read data in chunks
def read_data_in_chunks(file_name, chunk_size=1000):
    chunk = []
    with open(file_name, 'r', encoding='utf-8') as f:
        next(f)  # Skip the first line
        for i, line in enumerate(f):
            if (i % chunk_size == 0 and i > 0):
                yield chunk
                chunk = []
            chunk.append(line)
        yield chunk  # yield the last chunk, which may be smaller than chunk_size

# Adds IMDB data to the movies table in parallel processes
def update_moviesT_parrallel(file_name, chunk_size=1000):
    
    start_time = time.time()
    
    # Create a pool of processes 
    with Pool() as p:
        # Use the pool to process the data in parallel
        line_count = 0
        for chunk in read_data_in_chunks(file_name, chunk_size):
            line_count += len(chunk)
            #print(f'Processing lines {line_count - len(chunk) + 1} to {line_count}')
            results = p.map(process_data, [chunk])

            # Write the results to the database
            with sqlite3.connect(database_name) as conn:
                cursor = conn.cursor()
                for result in results[0]:
                    if file_name == imdb_movie_file:
                        # update movies table with result from process_data
                        Imdb_id = int(result[0][2:])
                        is_adult = None if result[4] == '\\N' else int(result[4])
                        runtime_min = None if result[7] == '\\N' else int(result[7])
                        if is_adult is not None or runtime_min is not None:
                            cursor.execute("UPDATE movies SET is_adult=?, runtime_min=? WHERE IMDB_id=?", (is_adult, runtime_min, Imdb_id))
                    
                    elif file_name == imdb_rating_file:
                        # update movies table with result from process_data
                        Imdb_id = int(result[0][2:])
                        avgRating = None if result[1] == '\\N' else float(result[1])
                        nVotes = None if result[2] == '\\N' else int(result[2])
                        if avgRating is not None or nVotes is not None:
                            cursor.execute("UPDATE movies SET avgRating_IMDB=?, numVotes_IMDB=? WHERE IMDB_id=?", (avgRating, nVotes, Imdb_id))
                conn.commit()
                #print(f'Updated {len(results[0])} rows')

    print(f'Finished in {time.time() - start_time} seconds')



def main():
    fill_movie_table()
    print('movies table created and populated, continue? (n to exit)')
    if input() == 'n':
        return
    fill_ratings_table()
    print('ratings table created and populated, continue? (n to exit)')
    if input() == 'n':
        return
    update_moviesT_parrallel(imdb_rating_file)
    print('IMDB data added to movies table, continue? (n to exit)')
    if input() == 'n':
        return
    update_moviesT_parrallel(imdb_movie_file)
    print('IMDB data added to movies table')

    print('Database created and populated')


if __name__ == '__main__':
    main()


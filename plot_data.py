import sqlite3
import matplotlib.pyplot as plt
import numpy as np  # Add the missing import statement for the numpy package
db = 'movie_recommender.db'

# Plot the distribution of ratings for each movie in the movies table. One distribution for the IMDb ratings and one for the User ratings.
def print_ratings():
    conn = sqlite3.connect(db)
    c = conn.cursor()

    c.execute("SELECT movie_id, avgRating_IMDB, avgRating_users FROM movies")
    rows = c.fetchall()

    imdb_ratings = [row[1] for row in rows if row[1] is not None]
    user_ratings = [row[2] for row in rows if row[2] is not None]

    plt.figure(1)
    plt.hist(imdb_ratings, bins=20, alpha=0.5, label='IMDb Ratings')
    plt.legend(loc='upper right')

    plt.figure(2)
    plt.hist(user_ratings, bins=20, alpha=0.5, label='User Ratings')
    plt.legend(loc='upper right')

    plt.show()

    conn.close()

if __name__ == '__main__':
    print_ratings()


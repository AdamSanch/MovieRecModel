.timer on
pragma foreign_keys = 1;

CREATE TABLE IF NOT EXISTS movies (
    movie_id INTEGER PRIMARY KEY, 
    IMDB_id INTEGER, 
    title TEXT, 
    genres TEXT, 
    is_adult INTEGER, 
    runtime_min INTEGER, 
    avgRating_IMDB REAL, 
    numVotes_IMDB INTEGER, 
    avgRating_users REAL, 
    numVotes_users INTEGER
);

CREATE TABLE IF NOT EXISTS ratings (
    user_id INTEGER, 
    movie_id INTEGER, 
    rating REAL, 
    timestamp INTEGER, 
    PRIMARY KEY (user_id, movie_id), 
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
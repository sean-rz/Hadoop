ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
    AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);
DUMP metadata;


nameLookup = FOREACH metadata GENERATE movieID, movieTitle,
    ToUnixTime(ToDate(releaseDate,'dd-MMM-yyyy')) AS releaseTime;
/* (1,Toy Story (1995),788918400) */


ratingsByMovie = GROUP ratings BY movieID;
/*
(1,{(807,1,4,892528231),(554,1,3,876231938),(49,1,2,888068651), … }
(2,{(429,2,3,882387599),(551,2,2,892784780),(774,2,1,888557383), … }
*/

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;
DESCRIBE avgRatings;
/*
(1,3.8783185840707963)
(2,3.2061068702290076)
(3,3.033333333333333)
(4,3.550239234449761)
(5,3.302325581395349)
*/

fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;
/*
(12,4.385767790262173)
(22,4.151515151515151)
(23,4.1208791208791204)
(45,4.05)
*/

fiveStarsWithData = JOIN fiveStarMovies BY movieID, nameLookup BY movieID;

/*
fiveStarMovies: {movieID: int,avgRating: double}
nameLookup: {movieID: int,movieTitle: chararray,releaseTime: long}
fiveStarsWithData: {fiveStarMovies::movieID: int,fiveStarMovies::avgRating: double,
nameLookup::movieID: int,nameLookup::movieTitle: chararray,nameLookup::releaseTime: long}

(12,4.385767790262173,12,Usual Suspects, The (1995),808358400)
(22,4.151515151515151,22,Braveheart (1995),824428800)
(23,4.1208791208791204,23,Taxi Driver (1976),824428800)
*/

oldestFiveStarMovies = ORDER fiveStarsWithData BY nameLookup::releaseTime;

DUMP oldestFiveStarMovies;
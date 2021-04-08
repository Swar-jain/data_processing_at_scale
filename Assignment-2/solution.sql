--query 1
CREATE TABLE query1 AS 
SELECT G.name AS name, count(H.genreid) AS moviecount
FROM hasagenre H, genres G
Where H.genreid = G.genreid
GROUP BY G.name
ORDER BY name ASC;

--query 2
CREATE TABLE query2 AS
SELECT G.name AS name, avg(R.rating) AS rating
FROM ratings R, genres G, hasagenre M
Where M.movieid = R.movieid AND M.genreID = G.genreID
GROUP BY G.name;

--query 3
CREATE TABLE query3 AS
SELECT m.title as title, count(r.rating) as CountOfRating
from ratings r, movies m 
WHERE m.movieid = r.movieid
GROUP BY m.movieid
HAVING count(r.rating)>10;

--query 4
CREATE TABLE query4 AS
SELECT M.movieid as movieid, M.title as title
FROM movies M, genres G, hasagenre H
WHERE G.genreid = H.genreid AND G.name = 'Comedy' AND H.movieid = M.movieid;

--query 5
CREATE TABLE query5 AS
Select M.title as title, avg(R.rating) as average
From movies M, ratings R
Where M.movieid = R.movieid
GROUP by M.title;

--query 6
CREATE TABLE query6 AS
Select avg(R.rating) as average
From  ratings R, hasagenre H, genres G
Where H.movieid = R.movieid and G.genreid = H.genreid and G.name = 'Comedy';

--query 7
CREATE TABLE query7 AS
SELECT avg(R.rating) as average 
FROM ratings R, genres g, hasagenre H 
where G.genreid = H.genreid and G.name = 'Comedy' AND G.name = 'Romance' and H.movieid = R.movieid;

--query 8
CREATE TABLE query8 AS
SELECT avg(r.rating) as average
FROM ratings r, genres g, hasagenre h
where g.genreid = h.genreid and h.movieid = r.movieid and g.name = 'Comedy' and g.genreid NOT IN (select genreid from genres where name = 'Romance');

--query 9 
CREATE TABLE query9 AS 
SELECT movieid as movieid , rating as rating FROM ratings 
WHERE userid = :v1;

--query 10
--Movies rated by user and their rating
CREATE TABLE query10A AS
SELECT movieid, rating
FROM ratings 
WHERE userid = :v1;

--Average rating per movie
CREATE TABLE query10B  AS
SELECT movieid as movieid, avg(rating) as rating
FROM ratings
GROUP BY movieid;

-- Similarity table
CREATE TABLE similarity AS
SELECT A.movieid as movieid1, B.movieid as movieid2, (1-(abs(A.rating - B.rating)/5)) as sim
FROM query10A A, query10B B
WHERE A.movieid != B.movieid;

-- Prediction table
CREATE TABLE P AS
SELECT s.movieid1 as movieid,
  CASE SUM(s.sim) WHEN 0.0 THEN 0.0
                  ELSE SUM(s.sim*A.rating)/SUM(s.sim)
  END as predictionscore
FROM similarity s, query10A A
WHERE s.movieid2 = A.movieid
AND s.movieid1 NOT IN (SELECT movieid FROM query10A)
GROUP BY s.movieid1 ORDER BY predictionscore DESC;

-- Recommendation table
CREATE TABLE recommendation AS
SELECT title
FROM movies M, P p
WHERE M.movieid = p.movieid
AND p.predictionscore>3.9;






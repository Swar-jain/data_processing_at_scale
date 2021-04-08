CREATE TABLE users(
    userid INTEGER UNIQUE NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY(userid)

);

CREATE TABLE movies(
    movieid INTEGER UNIQUE NOT NULL,
    title TEXT NOT NULL,
    PRIMARY KEY(movieid)

);

CREATE TABLE taginfo(
    tagid INTEGER UNIQUE NOT NULL,
    content TEXT NOT NULL,
    PRIMARY KEY(tagid)

);

CREATE TABLE genres(
    genreid INTEGER UNIQUE NOT NULL,
    name TEXT,
    PRIMARY KEY(genreid)

);

CREATE TABLE ratings(
    userid INTEGER NOT NULL,
    movieid INTEGER NOT NULL,
    rating NUMERIC CHECK(rating >= 0 and rating <= 5 ), 
    timestamp bigint,
    PRIMARY KEY ( userid, movieid),
    FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE on update cascade,
    FOREIGN KEY(userid) REFERENCES users ON DELETE cascade on update cascade
); 

CREATE TABLE tags(
    userid INTEGER NOT NULL ,
    movieid INTEGER NOT NULL,
    tagid INTEGER UNIQUE NOT NULL,
    timestamp bigint,
    PRIMARY KEY ( userid, movieid, tagid),
    FOREIGN KEY (userid) REFERENCES users ON DELETE cascade on update cascade,
    FOREIGN KEY(movieid) REFERENCES movies ON DELETE cascade on update cascade,
    FOREIGN KEY(tagid) REFERENCES taginfo ON DELETE no action
); 

CREATE TABLE hasagenre(
    movieid INTEGER,
    genreid INTEGER,
    PRIMARY KEY (movieid, genreid),
    FOREIGN KEY(movieid) REFERENCES movies ON DELETE cascade on update cascade,
    FOREIGN KEY(genreid) REFERENCES genres 
); 




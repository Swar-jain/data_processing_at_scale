#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    try:
        conn = openconnection.cursor()
        conn.execute("DROP TABLE IF EXISTS "+ratingstablename)
        conn.execute("CREATE TABLE "+ratingstablename+" (UserID INT, tempVar1 VARCHAR(10),  MovieID INT , tempVar2 VARCHAR(10),  Rating FLOAT, tempVar3 VARCHAR(10), Timestamp INT)")
        openconnection.commit()
        load = open(ratingsfilepath,'r')
        conn.copy_from(load ,ratingstablename,sep = ':')
        conn.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN tempVar1, DROP COLUMN tempVar2,DROP COLUMN tempVar3, DROP COLUMN Timestamp")
        openconnection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error : ", error)
    finally:
        if(openconnection):
            conn.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection.cursor()
    partition_no = 5 / numberofpartitions
    PREFIX = 'range_part'
    for i in range(0, numberofpartitions):
        lower_limit = i * partition_no
        upper_limit = lower_limit + partition_no
        new_table = PREFIX + str(i)
        conn.execute("DROP TABLE IF EXISTS "+ new_table)
        conn.execute("CREATE TABLE " + new_table + " (UserID INT, MovieID INT, Rating FLOAT)")
        if i == 0:
            conn.execute("INSERT INTO " + new_table + " SELECT UserID, MovieID, Rating FROM " + ratingstablename + " WHERE RATING >= " + str(lower_limit) + " AND RATING <= " + str(upper_limit))
        else:
            conn.execute("INSERT INTO " + new_table + " SELECT UserID, MovieID, Rating FROM " + ratingstablename + " WHERE RATING > " + str(lower_limit) + " AND RATING <= " + str(upper_limit))
    openconnection.commit()
    conn.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    try:
        conn = openconnection.cursor()
        conn.execute("ALTER TABLE "+ratingstablename+" ADD COLUMN ID SERIAL PRIMARY KEY")
        PREFIX = 'rrobin_part'
        for i in range(0, numberofpartitions):
            new_table = PREFIX + str(i)
            conn.execute("DROP TABLE IF EXISTS " + new_table)
            conn.execute("CREATE TABLE " + new_table + "(UserID INT, MovieID INT, Rating FLOAT, ID INT)")
            conn.execute("INSERT INTO " + new_table +"(UserID, MovieID, Rating, ID) SELECT * FROM "+ratingstablename+ " WHERE (id-1) % "+str(numberofpartitions)+" = "+str(i))
            conn.execute("ALTER TABLE "+ new_table + " DROP COLUMN ID")
        conn.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN ID")

        openconnection.commit()   
    except (Exception, psycopg2.DatabaseError) as error:
        print ("Error : ", error)
    finally:
        if(openconnection):
            conn.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection.cursor()
    conn.execute("SELECT COUNT(1) FROM pg_tables WHERE TABLENAME LIKE 'rrobin_part%'")
    numberofpartitions = conn.fetchone()[0]
    PREFIX = 'rrobin_part'
    conn.execute("INSERT INTO " + ratingstablename + "(UserID, MovieID, Rating) VALUES(%d, %d, %f)"%(userid, itemid, rating))
    conn.execute("select count(*) from " + ratingstablename)
    total = (conn.fetchall())[0][0]
    goto_partition = (total-1) % numberofpartitions
    new_table = PREFIX + str(goto_partition)
    conn.execute("INSERT INTO " + new_table + "(UserID, MovieID, Rating) VALUES(%d, %d, %f)"%(userid, itemid, rating))
    openconnection.commit()
    conn.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection.cursor()
    conn.execute("SELECT COUNT(1) FROM pg_tables WHERE TABLENAME LIKE 'range_part%'")
    count = conn.fetchone()[0]
    PREFIX = 'range_part'
    conn.execute("INSERT INTO " + ratingstablename + "(userid, movieid, rating) VALUES (%d, %d, %f)"%(userid, itemid, rating))
    partition_no = 5 / count
    goto_partition = int(rating / partition_no)
    if rating % partition_no == 0 and goto_partition != 0:
        goto_partition = goto_partition - 1
    new_table = PREFIX + str(goto_partition)
    conn.execute("INSERT INTO " + new_table + "(userid, movieid, rating) VALUES(%d, %d, %f)"%(userid, itemid, rating))
    openconnection.commit()
    conn.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()

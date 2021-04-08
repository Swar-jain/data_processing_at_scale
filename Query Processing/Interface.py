#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    conn = openconnection.cursor()
    #Extracting data from range partition
    conn.execute("select count(*) from information_schema.tables where table_name like 'rangeratingspart%'")
    count = conn.fetchone()[0]
    PREFIX = 'RangeRatingsPart'
    partition_no = 5 / count
    goto_partition = int(ratingMinValue / partition_no)
    stop_partition = int (ratingMaxValue/ partition_no)
    stop = stop_partition + 1
    if ratingMinValue % partition_no == 0 and goto_partition != 0:
        goto_partition = goto_partition - 1
    f = open("RangeQueryOut.txt", 'w')
    f.close()
    for i in range(goto_partition, stop):
        new_table = PREFIX + str(i)
        conn.execute("SELECT userid, movieid, rating from " + new_table + " WHERE rating BETWEEN " + str(ratingMinValue) + " AND " + str(ratingMaxValue))
        rows = conn.fetchall()
        f = open("RangeQueryOut.txt", 'a')
        for line in rows:
            f.write("RangeRatingsPart"+str(i)+",")
            f.write(','.join(str(s) for s in line))
            f.write('\n')
        

    #Extracting data from round partition
    conn.execute("select count(*) from information_schema.tables where table_name like 'roundrobinratingspart%'")
    count = conn.fetchone()[0]

    PREFIX = 'RoundRobinRatingsPart'
    partition_no = 5 / count
    for i in range(int(count)):
        new_table = PREFIX + str(i)
        conn.execute("SELECT userid, movieid, rating from " + new_table + " WHERE rating BETWEEN " + str(ratingMinValue) + " AND " + str(ratingMaxValue))
        rows = conn.fetchall()
        for line in rows:
            f.write("RoundRobinRatingsPart"+str(i)+",")
            f.write(','.join(str(s) for s in line))
            f.write('\n')
    f.close()
                
    openconnection.commit()

def PointQuery(ratingsTableName, ratingValue, openconnection):
    conn = openconnection.cursor()
    conn.execute("select count(*) from information_schema.tables where table_name like 'rangeratingspart%'")
    count = conn.fetchone()[0]
    #Extracting data from range partition
    PREFIX = 'RangeRatingsPart'
    partition_no = 5 / count
    f = open("PointQueryOut.txt", 'w')
    f.close()
    goto_partition = int(ratingValue / partition_no)
    new_table = PREFIX + str(goto_partition)
    if goto_partition == 0:
        new_table = PREFIX + str(0)
        conn.execute("SELECT userid, movieid, rating from " + new_table + " WHERE rating = " + str(ratingValue))
        rows = conn.fetchall()
        f = open("PointQueryOut.txt", 'a')
        for line in rows:
            f.write("RangeRatingsPart"+str(0)+",")
            f.write(','.join(str(s) for s in line))
            f.write('\n')
            print(line)
    else: 
        for i in range(goto_partition + 1):
            new_table = PREFIX + str(i)
            conn.execute("SELECT userid, movieid, rating from " + new_table + " WHERE rating = " + str(ratingValue))
            rows = conn.fetchall()
            f = open("PointQueryOut.txt", 'a')
            for line in rows:
                f.write("RangeRatingsPart"+str(i)+",")
                f.write(','.join(str(s) for s in line))
                f.write('\n')

    #Extracting data from round partition
    conn.execute("select count(*) from information_schema.tables where table_name like 'roundrobinratingspart%'")
    count = conn.fetchone()[0]
    PREFIX = 'RoundRobinRatingsPart'
    for i in range(count):
        new_table = PREFIX + str(i)
        conn.execute("SELECT userid, movieid, rating from " + new_table + " WHERE rating = " + str(ratingValue))
        rows = conn.fetchall()
        f = open("PointQueryOut.txt", 'a')
        for line in rows:
            f.write("RoundRobinRatingsPart"+str(i)+",")
            f.write(','.join(str(s) for s in line))
            f.write('\n')
        f.close()
            
            
    openconnection.commit()


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()

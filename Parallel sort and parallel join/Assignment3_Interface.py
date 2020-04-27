#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()

def helper(minL, maxL, table, column, openconnection, i):
    conn = openconnection.cursor()
    name = "temp"+str(i)
    conn.execute("DROP TABLE IF EXISTS " + name)
    if i==0:
        conn.execute("CREATE TABLE "+name+" AS SELECT * FROM "+table+" WHERE "+ column+" >= "+str(minL)+" AND "+ column+" <= "+str(maxL)+" ORDER BY "+ column+" ASC")
    else:
        conn.execute("CREATE TABLE "+name+" AS SELECT * FROM "+table+" WHERE "+ column+" > "+str(minL)+" AND "+ column+" <= "+str(maxL)+" ORDER BY "+ column +" ASC")
    
    
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    conn = openconnection.cursor()
    conn.execute("SELECT MAX("+SortingColumnName+") from "+InputTable)
    Max = conn.fetchone()[0]
    conn.execute("select MIN("+SortingColumnName+") from "+InputTable)
    Min = conn.fetchone()[0]
    partition_no = Max/5.0 
    lowerL = Min
    upperL = partition_no
    threads_no = 5
    threads = [0,0,0,0,0]
    for i in range(threads_no):
        threads[i] = threading.Thread(target = helper, args = (lowerL, upperL, InputTable, SortingColumnName, openconnection, i))
        threads[i].start()
        lowerL = upperL
        upperL = upperL + partition_no
    conn.execute("DROP TABLE IF EXISTS parallelsortoutputtable")
    conn.execute("CREATE TABLE " + OutputTable + " AS (SELECT * FROM "+InputTable+" WHERE 1=2)")
    for i in range(threads_no):
        threads[i].join()
        name = "temp"+str(i)
        conn.execute("INSERT INTO "+OutputTable+" SELECT * FROM "+name)
        conn.execute("DROP TABLE IF EXISTS "+name)
    openconnection.commit()

def joinHelper(Input1, Input2, Table1Column, Table2Column, Output, openconnection, low, Min, partition_no):
    conn = openconnection.cursor()
    if low == Min:
        conn.execute("INSERT INTO "+Output+" SELECT * FROM "+Input1+","+Input2+" WHERE "+Input1+"."+Table1Column+" >= "+str(low)+" AND "+Input2+"."+Table2Column+" >= "+str(low)+" and "+Input1+"."+Table1Column+" <= "+str(low+partition_no)+" and "+Input2+"."+Table2Column+" <= "+str(low+partition_no)+" and "+Input1+"."+Table1Column+" = "+Input2+"."+Table2Column)
    else:
        conn.execute("INSERT INTO "+Output+" SELECT * FROM "+Input1+","+Input2+" WHERE "+Input1+"."+Table1Column+" > "+str(low)+" AND "+Input2+"."+Table2Column+" > "+str(low)+" and "+Input1+"."+Table1Column+" <= "+str(low+partition_no)+" and "+Input2+"."+Table2Column+" <= "+str(low+partition_no)+" and "+Input1+"."+Table1Column+" = "+Input2+"."+Table2Column)


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    conn = openconnection.cursor()
    conn.execute("SELECT MAX("+Table1JoinColumn+"), MIN("+Table1JoinColumn+") FROM "+InputTable1)
    Max_Table1, Min_Table1 = conn.fetchone() 
    conn.execute("SELECT MAX("+Table2JoinColumn+"), MIN("+Table2JoinColumn+") FROM "+InputTable2)
    Max_Table2, Min_Table2 = conn.fetchone() 
    Max = max(Max_Table1, Max_Table2)
    Min = min(Min_Table1, Min_Table2)
    conn.execute("DROP TABLE IF EXISTS " + OutputTable)
    conn.execute("CREATE TABLE "+OutputTable+" AS (SELECT * FROM "+InputTable1+","+InputTable2+" where 1=2)")
    numofthreads = 5
    lower = Min
    partition_no = (Max-Min)/numofthreads
    threads = []
    while(lower<=Max):
        thread = threading.Thread(target= joinHelper, args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection, lower, Min ,partition_no))
        threads.append(thread)
        thread.start()
        lower = lower + partition_no
    for i in range(numofthreads):
        threads[i].join()
    openconnection.commit()
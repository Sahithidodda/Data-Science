### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    

    ### BEGIN SOLUTION

    with open(data_filename) as fi:
        read = fi.readlines()
        rgn_lst = []
        out_lst = []

        for el in range(1, len(read)):
            l_1= read[el].split("\t")
            regin = l_1[4]
            if regin not in rgn_lst:
                rgn_lst.append(regin)
        rgn_lst.sort()   

        for reg in rgn_lst:
            out_lst.append([reg])

    conn_norm = create_connection(normalized_database_filename)
    create_sql_region_table = '''CREATE TABLE [REGION](
                [RegionID] INTEGER PRIMARY KEY NOT NULL,
                [Region] TEXT NOT NULL)'''
    create_table(conn_norm, create_sql_region_table)

    with conn_norm:
        cur = conn_norm.cursor()
        sql = """INSERT INTO [REGION]([Region])
                VALUES(?)"""
        cur.executemany(sql, out_lst) 
    ### END SOLUTION

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    

    ### BEGIN SOLUTION

    outp_dict = {}
    region_sql_stat = '''SELECT * FROM [Region]'''
    conn_normalized = create_connection(normalized_database_filename)
    exc = execute_sql_statement(region_sql_stat, conn_normalized)

    for val in exc:
        outp_dict[val[1]] = val[0]
    return outp_dict

    ### END SOLUTION

def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    

    ### BEGIN SOLUTION

    with open(data_filename) as fi:
        read = fi.readlines()
        cnt_lst = [];cnt_dic = {}; 

        for el in range(1, len(read)):
            l_1= read[el].split("\t")
            cntry_nm = l_1[3]
            regn = l_1[4]

            if cntry_nm not in cnt_dic.keys():
                cnt_dic[cntry_nm] = regn

        out_dict = dict(sorted(cnt_dic.items(), key = lambda val:(val[0])))
        r_dic = step2_create_region_to_regionid_dictionary(normalized_database_filename)

        for i, j in out_dict.items():
            if j in r_dic.keys():
                cnt_lst.append([i, r_dic[j]])

    conn_normalized = create_connection(normalized_database_filename)
    create_sql_country_table = """CREATE TABLE [COUNTRY](
        [CountryID] INTEGER PRIMARY KEY NOT NULL,[Country] TEXT NOT NULL,[RegionID] INTEGER NOT NULL,
        FOREIGN KEY([RegionID]) REFERENCES [REGION]([RegionID]));"""
    create_table(conn_normalized, create_sql_country_table)

    with conn_normalized:
        cur = conn_normalized.cursor()
        sql_statement = '''INSERT INTO [COUNTRY]([Country], [RegionID]) 
                            VALUES (?,?)'''
        cur.executemany(sql_statement, cnt_lst) 
         
    ### END SOLUTION

def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    

    ### BEGIN SOLUTION

    sql_statement = '''SELECT * FROM [COUNTRY];'''
    conn_normalized = create_connection(normalized_database_filename) 
    r = execute_sql_statement(sql_statement, conn_normalized)

    cnt_dict = {}
    for row in r:
        cnt_dict[row[1]] = row[0]

    return cnt_dict
 
    ### END SOLUTION
        
def step5_create_customer_table(data_filename, normalized_database_filename):


    ### BEGIN SOLUTION
    with open(data_filename) as fi:
        read = fi.readlines();cust_lst = []
        cnt_dic = {}; leng=len(read); 

        for el in range(1, leng):
            l_1 = read[el].split("\t")
            first, last = l_1[0].split(" ",1)
            ad = l_1[1]
            ci = l_1[2]
            coun = l_1[3]  
            cnt_dic = step4_create_country_to_countryid_dictionary(normalized_database_filename)
            coun_id = cnt_dic.get(coun)
            cust_lst.append([first, last, ad, ci, coun_id])
        cust_lst = sorted(cust_lst, key = lambda val:(val[0], val[1]))

    conn_normalized = create_connection(normalized_database_filename); 
    create_sql_table_customer = '''CREATE TABLE [CUSTOMER](
        [CustomerID] INTEGER PRIMARY KEY NOT NULL,
        [FirstName] TEXT NOT NULL, [LastName] TEXT, [Address] TEXT, [City] TEXT,
        [CountryID] INTEGER NOT NULL,
        FOREIGN KEY([CountryID]) REFERENCES [COUNTRY]([CountryID]))'''

    create_table(conn_normalized, create_sql_table_customer)
    
    with conn_normalized:
        cur = conn_normalized.cursor()
        sql_statement = '''INSERT INTO [CUSTOMER]([FirstName], [LastName], 
                            [Address], [City], [CountryID])
                            VALUES (?,?,?,?,?)'''

        cur.executemany(sql_statement, cust_lst)


    ### END SOLUTION

def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    

    ### BEGIN SOLUTION
    
    conn_normalized = create_connection(normalized_database_filename)
    sql_statement = '''SELECT [FIRSTNAME] || " " || [LASTNAME] AS FULLNAME, 
                        [CUSTOMERID] FROM [CUSTOMER]'''
    
    ex = execute_sql_statement(sql_statement, conn_normalized)  
    
    cust_dict = {}
    for rw in ex:
        cust_dict[rw[0]] = rw[1]

    return cust_dict

    ### END SOLUTION
        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None


    ### BEGIN SOLUTION
    
    with open(data_filename) as fi:
        read = fi.readlines()
        leng = len(read)
        prd_lst = [];out_lst = []; 

        for el in range(1, leng):
            l_1= read[el].split("\t")
            prd_catg = l_1[6].split(";")
            prd_desc = l_1[7].split(';')
            zipped_lst = zip(prd_catg, prd_desc)
            
            for catg, de in zipped_lst:
                if catg not in prd_lst:
                    prd_lst.append(catg)
                    out_lst.append([catg, de])

            out_lst = sorted(out_lst, key = lambda val:(val[0]))
    
    conn_normalized = create_connection(normalized_database_filename)
    create__sql_table_productcategory = '''CREATE TABLE [ProductCategory]([ProductCategoryID] INTEGER PRIMARY KEY NOT NULL,
        [ProductCategory] TEXT NOT NULL, [ProductCategoryDescription] TEXT NOT NULL)'''

    create_table(conn_normalized, create__sql_table_productcategory)

    with conn_normalized:
        cur = conn_normalized.cursor()
        sql_statement = '''INSERT INTO [ProductCategory](
            [ProductCategory], [ProductCategoryDescription]) 
            VALUES (?,?)'''

        cur.executemany(sql_statement, out_lst) 
   

    ### END SOLUTION

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    

    ### BEGIN SOLUTION

    conn_normalized = create_connection(normalized_database_filename)
    sql_statement = '''SELECT * FROM [ProductCategory]'''
    ex = execute_sql_statement(sql_statement, conn_normalized)

    pd_catg_dict = {}
    for val in ex:
        pd_catg_dict[val[1]] = val[0]

    return pd_catg_dict 

    ### END SOLUTION
        
def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None


    ### BEGIN SOLUTION

    with open(data_filename) as fi:
        read = fi.readlines()
        leng = len(read)
        prd_lst = [];out_lst = []; 

        for el in range(1, leng):
            l_1 = read[el].split('\t') 
            prd = l_1[5]
            prd_catg = l_1[6]
            prd_pric = l_1[8].split(';')
            prd_dct = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)

            for x, y, z in zip(prd.split(';'), prd_pric,prd_catg.split(';')):
                if z in prd_dct:
                    if x not in out_lst:
                        out_lst.append(x)
                        prd_un_pric = str(round((float(y)),2))
                        prd_lst.append([x, prd_un_pric, prd_dct[z]])

        sorted_prd = sorted(prd_lst, key = lambda val:(val[0]))

    conn_normalized = create_connection(normalized_database_filename)
    create_sql_table_product = '''CREATE TABLE [Product]([ProductID] INTEGER NOT NULL PRIMARY KEY, 
                        [ProductName] TEXT NOT NULL,[ProductUnitPrice] Real NOT NULL,[ProductCategoryID] INTEGER NOT NULL,
                        FOREIGN KEY([ProductCategoryID]) REFERENCES [ProductCategory]([ProductCategoryID]))'''

    create_table(conn_normalized, create_sql_table_product)
    
    with conn_normalized:
        cur = conn_normalized.cursor()
        sql_statement = '''INSERT INTO [Product]([ProductName],
                        [ProductUnitPrice], [ProductCategoryID]) 
                        VALUES (?,?,?)'''

        cur.executemany(sql_statement, sorted_prd) 
   

    ### END SOLUTION

def step10_create_product_to_productid_dictionary(normalized_database_filename):
    

    ### BEGIN SOLUTION

    prd_dict = {}
    conn_normalized = create_connection(normalized_database_filename) 
    sql_statement = '''SELECT [ProductID], [ProductName]
                        FROM [Product]'''
    ex = execute_sql_statement(sql_statement, conn_normalized)

    for val in ex:
        prd_dict[val[1]] = val[0]

    return prd_dict

    ### END SOLUTION
        
def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    

    from datetime import datetime
    head = None;prd_lst = [];out_lst = []

    cust_dict = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
    prod_dict = step10_create_product_to_productid_dictionary(normalized_database_filename)

    with open(data_filename) as fi:
        for el in fi:
            if not el.strip():
                continue
            if not head:
                head = el.strip().split("\t")
                continue

            el = el.strip().split("\t")
            nme = el[0].split(";"); prd_nm = el[5].split(";"); 
            a = el[9].split(';'); ord = el[10].split(";"); 
            
            for rw in nme:
                for dt, pdnm, qty in zip(ord, prd_nm, a):
                    dt_con = datetime.strptime(dt, "%Y%m%d").strftime("%Y-%m-%d")
                    ex_tpl= (rw, pdnm,int(qty), dt_con)
                    prd_lst.append(ex_tpl)

    for a, s,p,o in prd_lst:
        r = cust_dict[a]
        t = prod_dict[s]
        outp = (r, t, o, p)
        out_lst.append(outp)

    conn_normalized = create_connection(normalized_database_filename)
    create_sql_table_orderdeta = '''CREATE TABLE [OrderDetail]([OrderID] INTEGER NOT NULL PRIMARY KEY,
        [CustomerID] INTEGER NOT NULL, [ProductID] INTEGER NOT NULL, [OrderDate] INTEGER NOT NULL,[QuantityOrdered] INTEGER NOT NULL, 
        FOREIGN KEY([CustomerID]) REFERENCES [Customer]([CustomerID]),FOREIGN KEY([ProductID]) REFERENCES [Product]([ProductID]))'''

    create_table(conn_normalized, create_sql_table_orderdeta)

    def prd_insrt(conn, values):
        sql_statement = '''INSERT INTO [OrderDetail]([CustomerID],
                    [ProductID],[OrderDate],[QuantityOrdered]) VALUES(?,?,?,?)'''
        cur = conn.cursor()
        cur.executemany(sql_statement, values)
        return cur.lastrowid

    with conn_normalized:
        prd_insrt(conn_normalized, out_lst) 

    ### END SOLUTION


def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    cust_dic = step6_create_customer_to_customerid_dictionary("normalized.db")

    for k, v in cust_dic.items():
        if k == CustomerName:
            cst_id = v
            break

    sql_statement = ('''SELECT ct.FirstName || " " || ct.LastName Name, prd.ProductName, ord.OrderDate, prd.ProductUnitPrice,
                        ord.QuantityOrdered, round(prd.ProductUnitPrice * ord.QuantityOrdered,2) AS Total
                        FROM OrderDetail ord INNER JOIN Product prd ON ord.ProductID = prd.ProductID INNER JOIN customer ct USING('CustomerID')
                        WHERE ct.CustomerID = '%s';''' % cst_id)
     
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    cust_dic = step6_create_customer_to_customerid_dictionary("normalized.db")

    for k, v in cust_dic.items():
        if k == CustomerName:
            cst_id = v
            break

    sql_statement = ('''SELECT ct.FirstName || " " || ct.LastName Name, round(sum(prd.ProductUnitPrice * ord.QuantityOrdered),2) AS Total 
                    FROM OrderDetail ord INNER JOIN Product prd ON ord.ProductID = prd.ProductID INNER JOIN customer ct USING('CustomerID')
                    WHERE ct.CustomerID = '%s' GROUP BY Name''' % cst_id )
    
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = '''SELECT Name, round(SUM(Total1),2) AS Total FROM(SELECT ct.FirstName || ' ' || ct.LastName AS Name, 
                        ProductUnitPrice * Quantityordered AS Total1 FROM OrderDetail ord INNER JOIN Product prd USING('ProductID') 
                        INNER JOIN CUSTOMER ct  ON ord.CustomerID = ct.CustomerID) 
                        GROUP BY Name ORDER BY Total DESC'''

    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement 
    

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = '''SELECT reg.Region, round(sum(prd.ProductUnitPrice * ord.QuantityOrdered),2) AS Total FROM OrderDetail ord 
                        INNER JOIN Customer ct USING('CustomerID') INNER JOIN Product prd on prd.ProductID  = ord.ProductID 
                        INNER JOIN Country cnt on cnt.CountryID = ct.CountryID  
                        INNER JOIN Region reg USING('RegionID')
                        GROUP BY reg.RegionID ORDER BY -Total'''

    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex5(conn):
    
     # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = '''SELECT cnt.Country, round(sum(prd.ProductUnitPrice * ord.QuantityOrdered)) AS CountryTotal 
                        FROM OrderDetail ord INNER JOIN Customer ct USING('CustomerID') 
                        INNER JOIN Product prd on prd.ProductID  = ord.ProductID 
                        INNER JOIN Country cnt on cnt.CountryID = ct.CountryID 
                        GROUP BY cnt.CountryID ORDER BY -CountryTotal'''

    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION

    sql_statement = '''SELECT *, rank() OVER(PARTITION BY Region ORDER BY CountryTotal DESC) CountryRegionalRank
                        FROM (SELECT rgn.Region, cnt.Country, round(sum(prd.ProductUnitPrice * ord.QuantityOrdered)) AS CountryTotal
                        FROM OrderDetail ord 
                        INNER JOIN Customer ct USING('CustomerID') 
                        INNER JOIN Product prd on prd.ProductID  = ord.ProductID 
                        INNER JOIN Country cnt on cnt.CountryID = ct.CountryID
                        INNER JOIN Region rgn USING('RegionID')
                        GROUP BY rgn.RegionID, cnt.CountryID ORDER BY Region)'''

    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
   # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    ### BEGIN SOLUTION

    sql_statement = '''SELECT * FROM(SELECT *, rank() OVER(PARTITION BY Region ORDER BY CountryTotal DESC) CountryRegionalRank
                        FROM (SELECT rgn.Region, cnt.Country, round(sum(prd.ProductUnitPrice * ord.QuantityOrdered)) AS CountryTotal
                        FROM OrderDetail ord 
                        INNER JOIN Customer ct USING('CustomerID') 
                        INNER JOIN Product prd on prd.ProductID  = ord.ProductID 
                        INNER JOIN Country cnt on cnt.CountryID = ct.CountryID
                        INNER JOIN Region rgn USING('RegionID') GROUP BY rgn.RegionID, cnt.CountryID ORDER BY Region, -CountryTotal))
                        WHERE CountryRegionalRank = 1'''
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    ### BEGIN SOLUTION

    sql_statement = """SELECT case 
                        WHEN 0 + strftime('%m', o.OrderDate) BETWEEN  1 AND  3 THEN 'Q1'
                        WHEN 0 + strftime('%m', o.OrderDate) BETWEEN  4 AND  6 THEN 'Q2'
                        WHEN 0 + strftime('%m', o.OrderDate) BETWEEN  7 AND  9 THEN 'Q3'
                        WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 10 AND 12 THEN 'Q4'
                        end AS Quarter, cast(strftime('%Y', o.OrderDate) AS INT) AS Year, c.CustomerID, round(sum(p.ProductUnitPrice*o.QuantityOrdered)) Total 
                        from OrderDetail o join customer c on c.CustomerID = o.CustomerID join Product p on p.ProductID = o.ProductID group by c.CustomerID,Year,Quarter Order by Year,Quarter,c.CustomerID"""
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    ### BEGIN SOLUTION

    sql_statement = '''WITH temp AS (
                        SELECT CASE
                        WHEN 0 + strftime('%m', od.OrderDate) BETWEEN 1 AND 3 THEN 'Q1'
                        WHEN 0 + strftime('%m', od.OrderDate) BETWEEN 4 AND 6 THEN 'Q2'
                        WHEN 0 + strftime('%m', od.OrderDate) BETWEEN 7 AND 9 THEN 'Q3'
                        WHEN 0 + strftime('%m', od.OrderDate) BETWEEN 10 AND 12 THEN 'Q4'
                        end AS Quarter, cast(strftime('%Y', od.OrderDate) AS INT) AS Year, cst.CustomerID, round(sum(pd.ProductUnitPrice*od.QuantityOrdered)) Total
                        FROM OrderDetail od 
                        INNER JOIN customer cst on cst.CustomerID = od.CustomerID
                        INNER JOIN Product pd USING('ProductID') 
                        GROUP BY cst.CustomerID,Year,Quarter ORDER BY Year, Total DESC)
                        SELECT * FROM (SELECT Quarter, Year, CustomerID, Total, ROW_NUMBER() OVER(PARTITION BY Year,Quarter order by Year,Quarter) CustomerRank FROM temp) WHERE CustomerRank BETWEEN 1 AND 5;'''

    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex10(conn):
    
    # Rank the monthy sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    ### BEGIN SOLUTION

    sql_statement = """WITH tb1 AS(
                        SELECT CASE
                        WHEN strftime('%m', od.OrderDate) = '01' THEN 'January'
                        WHEN strftime('%m', od.OrderDate) = '02' THEN 'February'
                        WHEN strftime('%m', od.OrderDate) = '03' THEN 'March'
                        WHEN strftime('%m', od.OrderDate) = '04' THEN 'April'
                        WHEN strftime('%m', od.OrderDate) = '05' THEN 'May'
                        WHEN strftime('%m', od.OrderDate) = '06' THEN 'June'
                        WHEN strftime('%m', od.OrderDate) = '07' THEN 'July'
                        WHEN strftime('%m', od.OrderDate) = '08' THEN 'August'
                        WHEN strftime('%m', od.OrderDate) = '09' THEN 'September'
                        WHEN strftime('%m', od.OrderDate) = '10' THEN 'October'
                        WHEN strftime('%m', od.OrderDate) = '11' THEN 'November'
                        WHEN strftime('%m', od.OrderDate) = '12' THEN 'December'
                        END Month, 
                        round(pd.ProductUnitPrice * od.QuantityOrdered,0) Total1
                        FROM OrderDetail od 
                        INNER JOIN Product pd ON od.ProductID = pd.ProductID),
                        tb2 AS(SELECT Month, round(Sum(Total1),0) Total FROM tb1 GROUP BY Month) SELECT Month, Total, ROW_NUMBER()OVER(ORDER BY -Total) TotalRank 
                        From tb2 ORDER BY TotalRank;"""
    
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    ### BEGIN SOLUTION

    sql_statement = """WITH tb1 AS(
                        SELECT cst.CustomerID, cst.FirstName, cst.LastName, c.Country, od.OrderDate,
                        Lag(od.OrderDate, 1) OVER(PARTITION BY cst.CustomerID
                        ORDER BY od.OrderDate) PreviousOrderDate
                        FROM OrderDetail od 
                        INNER JOIN Product pd USING('ProductID')
                        INNER JOIN Customer cst USING('CustomerID')
                        INNER JOIN Country c USING('CountryID')),
                        tb2 as(
                        SELECT 
                        CustomerID, FirstName, LastName, Country, OrderDate, PreviousOrderDate, 
                        MAX(JULIANDAY(OrderDate) - JULIANDAY(PreviousOrderDate)) MaxDaysWithoutOrder
                        FROM tb1 GROUP BY CustomerID ORDER BY MaxDaysWithoutOrder DESC)
                        SELECT CustomerID, FirstName, LastName, Country, OrderDate, PreviousOrderDate, MaxDaysWithoutOrder FROM tb2""" 
    
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
import requests
from operator import add


# run these line by line in pyspark 

# create spark configuration
conf = SparkConf()

# create spark context with the above configuration
sc = SparkContext(conf=conf)


# loading CSV input files into dataframes
customers = sqlContext.read.format('csv').options(header='true', inferSchema='true').load("Customers.csv")
orders = sqlContext.read.format('csv').options(header='true', inferSchema='true').load("Orders.csv")
products = sqlContext.read.format('csv').options(header='true', inferSchema='true').load("Products.csv")


customersRDD=customers.rdd
ordersRDD=orders.rdd
productsRDD=products.rdd


---------------------------------------------------------------------------

# Question 2
# Implement a pyspark code that can find out the top-10 sold products. 


# I prefer this rdd code 
ordersRDD.map(lambda p: (p[0], p[2]*p[3]) ).reduceByKey(lambda (a,b): a+b).top(10, lambda x: x[1])


# ---------------------------------------------------------------------------
# Question 3

# Find the top-10 customers based on the number of products ordered and consider that each order has a Volume of products.
# You should consider the volume of each order. In our dataset, the volume is just a decimal number that describes how many items is ordered of the specific product in that order.

ordersRDD.map(lambda p: (p[1], p[2]) ).reduceByKey(lambda (a,b): a+b).top(10, lambda x: x[1])

# ---------------------------------------------------------------------------

# Question 4
# Find the top-10 customers that have ordered products from the same supplier. 


# this returns the productID and customerID from the order table
productIDcustomerID = ordersRDD.map(lambda p: (p[2], p[1]) )

# this returns the productID and supplierID from product table 
productIDSupplierID = productsRDD.map(lambda p: (p[0] , p[6] )) 

productIDcustomerID.join(productIDSupplierID)\
                   .map(lambda x : ( ( x[1][0], x[1][1] ) , 1))\
                   .reduceByKey(lambda a, b: a+b)\
                   .top(10, lambda x: x[1]) 



# ---------------------------------------------------------------------------
# Question 5 and 6 
# Find the customers who have not ordered
# products from their own country and have ordered only foreign products. 

# Get customer and Orderd countries from product table 


customerCountries = productsRDD.map(lambda p: (p[0], p[4]))
				  
# It should be combined as a set - and not like what I have here 
customerOrderCountries = productIDcustomerID.join(customerCountries)\
                                            .map(lambda x: (x[1]))\
                                            .combineByKey(lambda x: [x], lambda u, v: u + [v], lambda u1,u2: u1+u2)\
                                            .map(lambda x:(x[0], set(x[1])) )

# Then we just check if it is inside or not and show the customer.
customerOrderCountries.join(customerCountries).filter(lambda x:  x[1][1] not in x[1][0]  ).take(5)



# ---------------------------------------------------------------------------
# Question 7
# Find the top-10 similar customers based of their orders. (Jaccard Similarity)
# First of all we collect all of the products that each customer ordered. 

customerIDproductIDs = ordersRDD.map(lambda p: (p[1], p[2]))\
                                .combineByKey(lambda x: [x], lambda u, v: u + [v], lambda u1,u2: u1+u2)\
                                .map(lambda x:(x[0], set(x[1])) )


customerIDproductIDs.cartesian(customerIDproductIDs)\
                    .filter(lambda a: a[0][0]!=a[1][0])\
                    .map(lambda a: ((a[0][0], a[1][0]), float( float( len(a[0][1] & a[1][1]))  / len(a[0][1] | a[1][1]) ) ))\
                    .top(10, lambda x: x[1])

# ---------------------------------------------------------------------------
# Question 8
# Implement a pyspark code that can find the top-10 products pairs that are ordered mostly together. 

from itertools import combinations

customerIDproductIDs.flatMap(lambda x:  combinations(x[1], 2) )
                    .map(lambda x: ((x[0], x[1] ), 1))
                    .reduceByKey(lambda a, b:a+b)
                    .top(10, lambda x: x[1])





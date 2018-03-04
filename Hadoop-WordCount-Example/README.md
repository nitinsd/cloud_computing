
# Source code location:
Tunrin/src/*.*

# Generated Classes location:
Tunrin/target/*.*

# JAR file location:
Tunrin/target/hw2-task3-0.2-SNAPSHOT-jar-with-dependencies.jar

# Results document:
Turnin/Result.docx

# POM file:
Turnin/pom.xml

# Install maven - compile 
To compile the project and create a single jar file with all dependencies: 	
	mvn clean compile assembly:single

# How to run hadoop job:	
Create different JARs with different Main class files so hadoop jobs can be run as a step on the cluster.
Then upload each Jar (one per task in assignment 2) is uploaded on S3. 
Large file is already uploaded on S3

Add the following step to the cluster:
JAR location :s3://nitin-assignment2/hw2-task1-0.2-SNAPSHOT-jar-with-dependencies.jar
Main class :None
Arguments :s3://metcs755/taxi-data-sorted-large.csv.bz2 s3://nitin-assignment2/task1/gpsErrorsLargejob1 s3://nitin-assignment2/task1/gpsErrorsLargejob2
Action on failure:Continue

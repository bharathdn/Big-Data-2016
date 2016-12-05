#input and output paths should be HDFS paths and not local

#input files where wikipedia pages are located
INPUT_FILE1 = "/user/bharathdn/hw3/input
#path where output should be written 
OUTPUT_FILE1A = "/user/bharathdn/hw3/output
#number of iterations to run the  pagerank
ITERATIONS = 10

build:
	mvn clean install

local:
	# Delete any existing output folders 
	hadoop fs -rm -r ${OUTPUT_FILE1A}
	# run the jar with Hadoop
	hadoop jar target/PageRank-0.0.1-SNAPSHOT.jar ${INPUT_FILE1} ${OUTPUT_FILE1A} ${ITERATIONS}
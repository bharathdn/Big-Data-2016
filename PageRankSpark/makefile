#input and output paths should be HDFS paths and not local

#input files where wikipedia pages are located
INPUT_FILE1 = "/user/bharathdn/hw4/input
#path where output should be written, output folder should not exist
OUTPUT_FILE1A = "/user/bharathdn/hw4/output
#number of iterations to run the  pagerank
ITERATIONS = 10
#number of top pages required
TOPPAGECOUNT = 100
#path to spark-submit's directory
SPARK_PATH = "/usr/local/Cellar/apache-spark/1.5.1/libexec/bin"

build:
	sbt package

local:
	# run the jar with Spark-Submit
	$SPARK_PATH/spark-submit --class "PageRank2" --master local[4] target/scala-2.11/pagerank_2.11-1.0.jar ${INPUT_FILE1} ${OUTPUT_FILE1A} ${TOPPAGECOUNT} ${ITERATIONS}
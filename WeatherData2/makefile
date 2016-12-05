#input and output paths should be HDFS paths and not local

#input fot Q1, all three versions
INPUT_FILE1 = "/user/bharathdn/Data/1912.csv"
#input for Q2, which contains 10 files for 10 different years
INPUT_FILE2 = "/user/bharathdn/Data/series"

#output for Q1 3 versions
OUTPUT_FILE1A = "/user/bharathdn/Result/1A"
OUTPUT_FILE1B = "/user/bharathdn/Result/1B"
OUTPUT_FILE1C = "/user/bharathdn/Result/1C"
#output for Q2 versions
OUTPUT_FILE2 = "/user/bharathdn/Result/2"

build:
	mvn clean install

local-1:
	# Delete any existing output folders 
	hadoop fs -rm -r ${OUTPUT_FILE1A}
	# run the jar with Hadoop
	hadoop jar target/WeatherData2-0.0.1-SNAPSHOT.jar ${INPUT_FILE1} ${OUTPUT_FILE1A} 1

local-2:
	#hadoop fs -rm -r ${OUTPUT_FILE1B}
	hadoop jar target/WeatherData2-0.0.1-SNAPSHOT.jar ${INPUT_FILE2} ${OUTPUT_FILE1B} 2

local-3:
	#hadoop fs -rm -r ${OUTPUT_FILE1C}
	hadoop jar target/WeatherData2-0.0.1-SNAPSHOT.jar ${INPUT_FILE3} ${OUTPUT_FILE1C} 3

local-4:
	#hadoop fs -rm -r ${OUTPUT_FILE2}
	hadoop jar target/WeatherData2-0.0.1-SNAPSHOT.jar ${INPUT_FILE2} ${OUTPUT_FILE2} 4
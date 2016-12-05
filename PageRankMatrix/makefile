#input and output paths should be HDFS paths and not local

#input files where readily parsed adjacency list files are located 

# NOTE: the path should have a folder called "links" which contains the input file/partfiles
# if "/user/bharathdn/hw5/input" is input path, then there should be an
# existing path such as "/user/bharathdn/hw5/input/links"

INPUT_FILE = "/user/bharathdn/hw5/input"

#version A = 1, B = 2 
VERSION = 1

build:
	mvn clean install

local:
	# run the jar with Hadoop
	hadoop jar target/PageRankMatrix-0.0.1-SNAPSHOT.jar ${INPUT_FILE} ${VERSION}
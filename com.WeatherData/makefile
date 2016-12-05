INPUT_FILE = "Input/1991.csv"
THREADCOUNT = 4

build:
	mvn clean install

local-1:
	java -jar target/com.WeatherData-0.0.1-SNAPSHOT.jar 1 ${THREADCOUNT} ${INPUT_FILE}

local-2:		
	java -jar target/com.WeatherData-0.0.1-SNAPSHOT.jar 2 ${THREADCOUNT} ${INPUT_FILE}

local-3:
	java -jar target/com.WeatherData-0.0.1-SNAPSHOT.jar 3 ${THREADCOUNT} ${INPUT_FILE}

local-4:		
	java -jar target/com.WeatherData-0.0.1-SNAPSHOT.jar 4 ${THREADCOUNT} ${INPUT_FILE}

local-5:
	java -jar target/com.WeatherData-0.0.1-SNAPSHOT.jar 5 ${THREADCOUNT} ${INPUT_FILE}

local:
	#make build
	make local-1
	make local-2
	make local-3
	make local-4
	make local-5

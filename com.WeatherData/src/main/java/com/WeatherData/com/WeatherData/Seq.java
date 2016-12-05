package com.WeatherData.com.WeatherData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Seq {

	/*
	 * Constants declaration
	 */
	private static final String TMAX = "TMAX";
	// number of times, average is calculated in a loop
	private static int LOOPCOUNT = 10;
	// index of TMAX in the csv
	private static final int TMAXINDEX = 3;
	// index of STATIONID in the csv
	private static final int STATIONIDINDEX = 0;
	//output file path
	private static final String outPutFilePath = "out/SeqOutput.txt";

	//	Accumulation Data Structure
	private HashMap<String, StationAttribute> stationData = new HashMap<String, StationAttribute>();
	// stores runtimes of parsing and computing 
	private List<Long> runTime = new ArrayList<Long>();


	private void parseContents(List<String> fileContents) {
		for (String line : fileContents) {
			if(line.contains(TMAX))
				updateTMax(line);
		}
	}

	// if the input has the word TMAX, then updated the corresponding TMAX value to the accumulation data structure
	private void updateTMax(String line) {
		String[] lineContents = line.split(",");
		int tMax;
		try {
			tMax = Integer.valueOf(lineContents[TMAXINDEX]);
		} 
		catch (NumberFormatException e) {
			return;
		}

		StationAttribute sb; 

		if(stationData.containsKey(lineContents[STATIONIDINDEX])) {
			sb = stationData.get(lineContents[STATIONIDINDEX]);
			sb.settMaxSum(sb.gettMaxSum() + tMax);
			sb.setTMaxCount(sb.getTMaxCount() + 1);
		}
		else {
			sb = new StationAttribute(tMax, 1);
			stationData.put(lineContents[STATIONIDINDEX], sb);
		}
	}


	// Prints the Computed Average to a file
	private void printResults(int runCount) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n\nRun"+ runCount + "\n StationID :: "+ "\t" + "TMax Avg\n");
		for(Map.Entry<String, StationAttribute> entry : stationData.entrySet()) {
			sb.append(entry.getKey() + "\t\t\t" + entry.getValue().getTMaxAverage() +"\n");
		}
		DataParserHelper.WriteToFile(sb.toString(), outPutFilePath);
	}


	private void computeAvgMgr(List<String> contents) {
		parseContents(contents);
		computeAverage();
	}


	private void computeAverage() {
		for(Map.Entry<String, StationAttribute> entry : stationData.entrySet()) {
			int temperatureSum = entry.getValue().gettMaxSum();
			int temperatureCount = entry.getValue().getTMaxCount();
			entry.getValue().setTMaxAverage((float)temperatureSum/temperatureCount);
		}	
	}


	public void performTmaxComputation(String inputPath) {

		System.out.println("\n========******** Executing Sequentially *******=========");
		System.out.println("\nStarted loading data ");
		List<String> contents = DataLoader.loadFromFile(inputPath);

		// Delete previous result files
		DataParserHelper.deleteFile(outPutFilePath);

		System.out.println("Running Without Fibonacci");
		StationAttribute.setcalculateFib(false);
		TmaxComputationHelper(contents);
		DataParserHelper.printTimeStatistics(runTime);

		// clear the Data Structures, as the same object is being used
		runTime.clear();

		System.out.println("\n\nRunning With Fibonacci");
		StationAttribute.setcalculateFib(true);
		TmaxComputationHelper(contents);
		DataParserHelper.printTimeStatistics(runTime);
	} 


	public void TmaxComputationHelper(List<String> contents) {
		for (int i = 0; i < LOOPCOUNT; i++) {

			long startTime = System.currentTimeMillis();		
			computeAvgMgr(contents);				
			long elapsedTime = System.currentTimeMillis() - startTime;

			printResults(i+1);
			runTime.add(elapsedTime);

			// clear the Data Structures, as the same object is being used
			stationData.clear();
		}
	}
}

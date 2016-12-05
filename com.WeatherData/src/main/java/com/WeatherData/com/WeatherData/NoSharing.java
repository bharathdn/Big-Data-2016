package com.WeatherData.com.WeatherData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NoSharing implements Runnable {

	/*
	 * Constants declaration
	 */
	// index of TMAX in the csv
	private static int THREADCOUNT = 4;
	private static final String TMAX = "TMAX";
	// number of times, average is calculated in a loop
	private static int LOOPCOUNT = 10;
	// index of TMAX in the csv
	private static final int TMAXINDEX = 3;
	// index of STATIONID in the csv
	private static final int STATIONIDINDEX = 0;
	//output file path
	private static final String outPutFilePath = "out/NoSharingOutput.txt";

	/*
	 * Accumulation Data Structure
	 */ 
	private Map<String, StationAttribute> stationData = new HashMap<String, StationAttribute>();

	// Data structure to collect running times of avg computation
	private List<Long> runTime = new ArrayList<Long>();
	// data structure of lists of lists, where each list contains lines from the input file
	private List<List<String>> dividedInputList = new ArrayList<List<String>>(THREADCOUNT);
	private List<String> threadInputdata;

	private Map<String, StationAttribute> threadAccumulator;
	private List<Map<String, StationAttribute>> accumulator = new ArrayList<>();

	public NoSharing(){

	}


	public NoSharing(List<String> data, Map<String, StationAttribute> threadAccumultor) {
		this.threadInputdata = data;
		this.threadAccumulator = threadAccumultor;
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


	private void computeAverage() {
		for(Map.Entry<String, StationAttribute> entry : stationData.entrySet()) {
			int temperatureSum = entry.getValue().gettMaxSum();
			int temperatureCount = entry.getValue().getTMaxCount();
			entry.getValue().setTMaxAverage((float)temperatureSum/temperatureCount);
		}	
	}


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
		if(threadAccumulator.containsKey(lineContents[STATIONIDINDEX])) {
			sb = threadAccumulator.get(lineContents[STATIONIDINDEX]);
			sb.settMaxSum(sb.gettMaxSum() + tMax);
			sb.setTMaxCount(sb.getTMaxCount() + 1);
		}
		else {
			sb = new StationAttribute(tMax, 1);
			threadAccumulator.put(lineContents[STATIONIDINDEX], sb);
		}
	}


	private void parseContents() {
		for (String line : threadInputdata)
			if(line.contains(TMAX))
				this.updateTMax(line);
	}


	@Override
	public void run() {
		parseContents();
	}


	private void combineResults() {
		int combinedCount = 0;
		StationAttribute sb;
		Map<String, StationAttribute> map;

		while(true){
			map = accumulator.get(combinedCount);
			for(Map.Entry<String, StationAttribute> threadAccumulatorEntry : map.entrySet()) {
				String stationId = threadAccumulatorEntry.getKey();			
				int temperatureSum = threadAccumulatorEntry.getValue().gettMaxSum();
				int temperatureCount = threadAccumulatorEntry.getValue().getTMaxCount();

				if(stationData.containsKey(stationId)){
					sb = stationData.get(stationId);
					sb.settMaxSum(sb.gettMaxSum() + temperatureSum );
					sb.setTMaxCount(sb.getTMaxCount() + temperatureCount);				
				} else {
					sb = new StationAttribute(temperatureSum ,temperatureCount);
					stationData.put(stationId, sb);
				}
			}

			if(++combinedCount == THREADCOUNT)
				return;
		}
	}


	public void scheduleToThreads() throws InterruptedException {

		NoSharing[] noSharing = new NoSharing[THREADCOUNT];
		Thread[] tMaxThreads = new Thread[THREADCOUNT];

		for (int i = 0; i < THREADCOUNT; i++) {
			Map<String, StationAttribute> threadAccumulator = new HashMap<>();
			accumulator.add(threadAccumulator);
			noSharing[i] = new NoSharing(dividedInputList.get(i), threadAccumulator);
			tMaxThreads[i] = new Thread(noSharing[i]);
			tMaxThreads[i].start();
		}

		for (int i = 0; i < THREADCOUNT; i++) {
			tMaxThreads[i].join();
		}
	}


	public void performTmaxComputation(int threadCount, String inputPath) {
		THREADCOUNT = threadCount;
		
		System.out.println("\n========******** Executing No Sharing *******=========");
		System.out.println("\nStarted loading data ");
		dividedInputList = DataLoader.loadIntoMultipleList(inputPath, THREADCOUNT);

		// Delete previous result files
		DataParserHelper.deleteFile(outPutFilePath);

		System.out.println("Running Without Fibonacci");
		StationAttribute.setcalculateFib(false);
		TmaxComputationHelper();
		DataParserHelper.printTimeStatistics(runTime);

		// clear the Data Structures, as the same object is being used
		runTime.clear();

		System.out.println("\n\nRunning With Fibonacci");
		StationAttribute.setcalculateFib(true);
		TmaxComputationHelper();
		DataParserHelper.printTimeStatistics(runTime);	
	}


	public void TmaxComputationHelper() {
		for (int i = 0; i < LOOPCOUNT; i++) {

			long startTime = System.currentTimeMillis();		
			try {
				scheduleToThreads();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}				
			combineResults();
			computeAverage();
			long elapsedTime = System.currentTimeMillis() - startTime;

			printResults(i+1);
			runTime.add(elapsedTime);

			// clear the Data Structures, as the same object is being used
			stationData.clear();
		}
	}
}

package com.WeatherData.com.WeatherData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FineLock implements Runnable {

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
	private static final String outPutFilePath = "out/FineLockOutput.txt";

	/*
	 * Accumulation Data Structure
	 */ 
	private static Map<String, StationAttribute> stationData = new ConcurrentHashMap<String, StationAttribute>();

	// Data structure to collect running times of avg computation
	private List<Long> runTime = new ArrayList<Long>();
	// data structure of lists of lists, where each list contains lines from the input file
	private List<List<String>> dividedInputList = new ArrayList<List<String>>(THREADCOUNT);
	private List<String> threadInputdata;
	
	/*
	 * Locks
	 */
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

	public FineLock(){

	}


	public FineLock(List<String> data){
		this.threadInputdata = data;
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


	// Computes the average from sum and count for each stationId
	private void computeAverage() {
		for(Map.Entry<String, StationAttribute> entry : stationData.entrySet()) {
			int temperatureSum = entry.getValue().gettMaxSum();
			int temperatureCount = entry.getValue().getTMaxCount();
			entry.getValue().setTMaxAverage((float)temperatureSum/temperatureCount);
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
		
		boolean updated = false;
		do {
			// Because finelock does not use, setters from StationAttribute class,
			// explicitly calculate fibonacci
			if(StationAttribute.getcalculateFib()) 
				DataParserHelper.fibonacci(17);
			
			StationAttribute sb = stationData.putIfAbsent(lineContents[STATIONIDINDEX], new StationAttribute(tMax, 1));
			
			if(sb == null){
				updated = true;
			} else {
				sb = stationData.replace(lineContents[STATIONIDINDEX],  
						new StationAttribute(sb.gettMaxSum() + tMax, sb.getTMaxCount() + 1));
				if(sb != null)
					updated = true;
			}
		} while(! updated);

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


	public void scheduleToThreads() throws InterruptedException {

		FineLock[] fineLock = new FineLock[THREADCOUNT];
		Thread[] tMaxThreads = new Thread[THREADCOUNT];

		for (int i = 0; i < THREADCOUNT; i++) {
			fineLock[i] = new FineLock(dividedInputList.get(i));
			tMaxThreads[i] = new Thread(fineLock[i]);
			tMaxThreads[i].start();
			tMaxThreads[i].setName("Thread"+1);
		}

		for (int i = 0; i < THREADCOUNT; i++) {
			tMaxThreads[i].join();
		}
	}


	public void performTmaxComputation(int threadCount, String inputPath) {
		THREADCOUNT = threadCount;

		System.out.println("\n========******** Executing Fine Lock *******=========");
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
			computeAverage();
			long elapsedTime = System.currentTimeMillis() - startTime;

			printResults(i+1);
			runTime.add(elapsedTime);

			// clear the Data Structures, as the same object is being used
			stationData.clear();
		}
	}
}

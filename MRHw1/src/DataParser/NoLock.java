package DataParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NoLock implements Runnable { 

	/*
	 * Constants declaration
	 */
	// index of TMAX in the csv
	private static final int TMAXINDEX = 3;
	private static final String TMAX = "TMAX";
	private static final int LOOPCOUNT = 10;
	private static int THREADCOUNT = 4;



	/*
	 * Accumulation Data Structure
	 */ 
	private static HashMap<String, StationAttribute> stationData = new HashMap<String, StationAttribute>();
	
	private static List<Long> runTime = new ArrayList<Long>();
	private List<List<String>> dividedList = new ArrayList<List<String>>(THREADCOUNT);
	private List<String> data;
	
	public NoLock(){
		
	}

	public NoLock(List<String> data){
		this.data = data;
	}

	private void printResults() {
		for(Map.Entry<String, StationAttribute> entry : stationData.entrySet()) {
			System.out.println("StationId: " + entry.getKey() + " TMaxAvg: " + entry.getValue().getTemperatureAverage());
		}
	}

	private void computeAverage() {
		for(Map.Entry<String, StationAttribute> entry : stationData.entrySet()) {
			int temperatureSum = entry.getValue().getTemperatureSum();
			int temperatureCount = entry.getValue().getTemperatureCount();
			entry.getValue().setTemperatureAverage((float)temperatureSum/temperatureCount);
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

		if(stationData.containsKey(lineContents[0])) {
			sb = stationData.get(lineContents[0]);
			sb.setTemperatureSum(sb.getTemperatureSum() + tMax);
			sb.setTemperatureCount(sb.getTemperatureCount() + 1);
		}
		else {
			sb = new StationAttribute(tMax, 1);
			stationData.put(lineContents[0], sb);
		}
	}

	
	private void parseContents() {
		for (String line : data)
			if(line.contains(TMAX))
				this.updateTMax(line);
	}

	
	@Override
	public void run() {
		parseContents();
	}

	public void scheduleToThreads() throws InterruptedException {
		
		NoLock[] noLocks = new NoLock[THREADCOUNT];
		Thread[] tMaxThreads = new Thread[THREADCOUNT];
		
		for (int i = 0; i < THREADCOUNT; i++) {
			noLocks[i] = new NoLock(dividedList.get(i));
			tMaxThreads[i] = new Thread(noLocks[i]);
			tMaxThreads[i].start();
		}
		
		for (int i = 0; i < THREADCOUNT; i++) {
			tMaxThreads[i].join();
		}
	}

	public static void main(String[] args) throws InterruptedException {
		String filePath1763 = "/Users/bharathdn/Documents/MR/1763.csv";
		//String filePath1912 = "/Users/bharathdn/Documents/MR/1912.csv";

		System.out.println("\n\nStarted loading data ");
		NoLock noLock = new NoLock();
		noLock.dividedList = DataLoader.loadIntoMultipleList(filePath1763, THREADCOUNT);
		
		for (int i = 0; i < LOOPCOUNT; i++) {
			
			long startTime = System.currentTimeMillis();
			noLock.scheduleToThreads();				
			noLock.computeAverage();
			long elapsedTime = System.currentTimeMillis() - startTime;
			
			noLock.printResults();
			runTime.add(elapsedTime);
			
			// clear the Data Structures, as the same object is being used
			stationData.clear();
		}
		
		DataParserHelper.printTimeStatistics(runTime);
	}
}

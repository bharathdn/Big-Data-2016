package DataParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FineLock implements Runnable {

	/*
	 * Constants declaration
	 */
	// index of TMAX in the csv
	private static final int TMAXINDEX = 3;
	private static final int STATIONIDINDEX = 0;
	private static final String TMAX = "TMAX";
	private static final int LOOPCOUNT = 3;
	private static  int THREADCOUNT = 4;

	/*
	 * Accumulation Data Structure
	 */ 
	private static Map<String, StationAttribute> stationData = 
			new ConcurrentHashMap<String, StationAttribute>(16, .75f, THREADCOUNT);

	private static List<Long> runTime = new ArrayList<Long>();
	private List<List<String>> dividedList = new ArrayList<List<String>>(THREADCOUNT);
	private List<String> data;

	public FineLock(){

	}


	public FineLock(List<String> data){
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
			System.out.println(temperatureSum);
			System.out.println(temperatureCount);
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

//		System.out.println(tMax);

		//Source: http://stackoverflow.com/questions/28937880/synchronized-on-hashmap-value-object

		StationAttribute sb;
		if(stationData.containsKey(lineContents[0])) {
			sb = stationData.get(lineContents[0]);
			sb.setTemperatureSum(sb.getTemperatureSum() + tMax);
			sb.setTemperatureCount(sb.getTemperatureCount() + 1);	
		}
		else {
			sb = new StationAttribute(tMax, 1);
			stationData.putIfAbsent(lineContents[0], sb);
		}

//		boolean updated = false;
//		do {
//			StationAttribute sb = stationData.putIfAbsent(lineContents[STATIONIDINDEX], new StationAttribute(tMax, 1));
//
//			if(sb == null){
//				updated = true;
//			} else {
//				sb = stationData.replace(lineContents[STATIONIDINDEX],  
//						new StationAttribute(sb.getTemperatureSum() + tMax, sb.getTemperatureCount() + 1));
//				if(sb != null)
//					updated = true;
//			}
//		} while(! updated);

	}



	private void printHashMap() {
		System.out.println("\nPrint start");
		for(Map.Entry<String, StationAttribute> entry : stationData.entrySet()) {
			System.out.println(entry.getKey() + " " 
					+ entry.getValue().getTemperatureSum() +  " " + entry.getValue().getTemperatureCount());
		}
		System.out.println("\nPrint end");
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

		FineLock[] fineLock = new FineLock[THREADCOUNT];
		Thread[] tMaxThreads = new Thread[THREADCOUNT];

		for (int i = 0; i < THREADCOUNT; i++) {
			fineLock[i] = new FineLock(dividedList.get(i));
			tMaxThreads[i] = new Thread(fineLock[i]);
			tMaxThreads[i].start();
			tMaxThreads[i].setName("Thread"+1);
		}

		for (int i = 0; i < THREADCOUNT; i++) {
			tMaxThreads[i].join();
		}
	}


	public static void main(String[] args) throws InterruptedException {
		//String filePath1763 = "/Users/bharathdn/Documents/MR/1763.csv";
		//String filePath1912 = "/Users/bharathdn/Documents/MR/1912.csv";
		String filePath1912 = "/Users/bharathdn/Documents/MR/test2.csv";

		System.out.println("\n\nStarted loading data ");		 

		FineLock fineLock; //= new FineLock();
		//		fineLock.dividedList = DataLoader.loadIntoMultipleList(filePath1912, THREADCOUNT);

		for (int i = 0; i < LOOPCOUNT; i++) {
			fineLock = new FineLock();
			fineLock.dividedList = DataLoader.loadIntoMultipleList(filePath1912, THREADCOUNT);
			long startTime = System.currentTimeMillis();
			fineLock.scheduleToThreads();				
			fineLock.computeAverage();
			long elapsedTime = System.currentTimeMillis() - startTime;

			fineLock.printResults();
			runTime.add(elapsedTime);

			// clear the Data Structures, as the same object is being used
			stationData.clear();

		}

		DataParserHelper.printTimeStatistics(runTime);
	}
}

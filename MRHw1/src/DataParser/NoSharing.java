package DataParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NoSharing implements Runnable {

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
	private Map<String, StationAttribute> stationData = new HashMap<String, StationAttribute>();

	private static List<Long> runTime = new ArrayList<Long>(THREADCOUNT);

	private List<List<String>> dividedList = new ArrayList<List<String>>(THREADCOUNT);
	private List<String> data;

	private Map<String, StationAttribute> threadAccumulator;
	private List<Map<String, StationAttribute>> accumulator = new ArrayList<>();

	//private static Object lock = new Object();

	public NoSharing(){

	}


	public NoSharing(List<String> data, Map<String, StationAttribute> threadAccumultor) {
		this.data = data;
		this.threadAccumulator = threadAccumultor;
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
		if(threadAccumulator.containsKey(lineContents[0])) {
			sb = threadAccumulator.get(lineContents[0]);
			sb.setTemperatureSum(sb.getTemperatureSum() + tMax);
			sb.setTemperatureCount(sb.getTemperatureCount() + 1);
		}
		else {
			sb = new StationAttribute(tMax, 1);
			threadAccumulator.put(lineContents[0], sb);
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


	private void combineResults() {
		int combinedCount = 0;
		StationAttribute sb;
		Map<String, StationAttribute> map;

		while(true){
			map = accumulator.get(combinedCount);
			for(Map.Entry<String, StationAttribute> threadAccumulatorEntry : map.entrySet()) {
				String stationId = threadAccumulatorEntry.getKey();			
				int temperatureSum = threadAccumulatorEntry.getValue().getTemperatureSum();
				int temperatureCount = threadAccumulatorEntry.getValue().getTemperatureCount();

				if(stationData.containsKey(stationId)){
					sb = stationData.get(stationId);
					sb.setTemperatureSum(sb.getTemperatureSum() + temperatureSum );
					sb.setTemperatureCount(sb.getTemperatureCount() + temperatureCount);				
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
			noSharing[i] = new NoSharing(dividedList.get(i), threadAccumulator);
			tMaxThreads[i] = new Thread(noSharing[i]);
			tMaxThreads[i].start();
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

		NoSharing noSharing = new NoSharing();
		noSharing.dividedList = DataLoader.loadIntoMultipleList(filePath1912, THREADCOUNT);

		for (int i = 0; i < LOOPCOUNT; i++) {

			long startTime = System.currentTimeMillis();
			noSharing.scheduleToThreads();
			noSharing.combineResults();
			noSharing.computeAverage();
			long elapsedTime = System.currentTimeMillis() - startTime;

			noSharing.printResults();
			runTime.add(elapsedTime);

			// clear the Data Structures, as the same object is being used
			noSharing.stationData.clear();
		}

		DataParserHelper.printTimeStatistics(runTime);
	}
}

package DataParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SeqExecution {

	/*
	 * Constants declaration
	 */
	private static final String TMAX = "TMAX";
	private static final int LOOPCOUNT = 10;
	// index of TMAX in the csv
	private static final int TMAXINDEX = 3;


	/*
	 * Accumulation Data Structure
	 */ 
	private HashMap<String, StationAttribute> stationData = new HashMap<String, StationAttribute>();

	private List<Long> runTime = new ArrayList<Long>();

	/*
	 * Takes ArrayList<Strings> and fetches TMax
	 * i/p : ArrayList<Strings>
	 * o/p : 
	 */
	private void parseContents(List<String> fileContents) {
		for (String line : fileContents) {
			if(line.contains(TMAX))
				updateTMax(line);
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

	private void printResults() {
		for(Map.Entry<String, StationAttribute> entry : stationData.entrySet()) {
			System.out.println("StationId: " + entry.getKey() + " TMaxAvg: " + entry.getValue().getTemperatureAverage());
		}
	}

	private void computeAvgMgr(List<String> contents) {
		parseContents(contents);
		computeAverage();
	}

	private void computeAverage() {
		for(Map.Entry<String, StationAttribute> entry : stationData.entrySet()) {
			int temperatureSum = entry.getValue().getTemperatureSum();
			int temperatureCount = entry.getValue().getTemperatureCount();
			entry.getValue().setTemperatureAverage((float)temperatureSum/temperatureCount);
		}	
	}

	public void performTmaxComputation(boolean fibonacciCalculation) {

		String filePath1763 = "/Users/bharathdn/Documents/MR/1763.csv";
		//String filePath1912 = "/Users/bharathdn/Documents/MR/1912.csv";
//		String filePath1912 = "/Users/bharathdn/Documents/MR/test2.csv";


		System.out.println("\nStarted loading data ");
		List<String> contents = DataLoader.loadFromFile(filePath1763);
		StationAttribute.setcalculateFib(fibonacciCalculation);

		for (int i = 0; i < LOOPCOUNT; i++) {

			long startTime = System.currentTimeMillis();		
			computeAvgMgr(contents);				
			long elapsedTime = System.currentTimeMillis() - startTime;

			printResults();
			runTime.add(elapsedTime);

			// clear the Data Structures, as the same object is being used
			stationData.clear();
		}

		DataParserHelper.printTimeStatistics(runTime);
	} 

	public static void main(String[] args) {
		SeqExecution seq = new SeqExecution();
		seq.performTmaxComputation(false);
		System.out.println("\n\n\ncalculating with Fib\n\n\n");
		
		SeqExecution seq2 = new SeqExecution();
		seq2.performTmaxComputation(true);
	}
}

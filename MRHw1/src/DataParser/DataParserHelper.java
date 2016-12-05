package DataParser;

import java.util.ArrayList;
import java.util.List;

public class DataParserHelper {

	/*
	 *  Method that divides data in List of strings into
	 *  multiple lists. 
	 */
	public static List<List<String>> divideList(List<String> contents, int numberOfLists) {

		List<List<String>> dividedList = new ArrayList<List<String>>();
		int partsSize = contents.size() / numberOfLists;
		System.out.println(partsSize);

		List<String> newList = new ArrayList<String>();
		for (int i = 0; i < contents.size(); i++) {
			newList.add(contents.get(i));

			if((dividedList.size() < numberOfLists - 1) && (newList.size() ==  partsSize)) {
				dividedList.add(newList);
				newList = new ArrayList<String>();
				continue;
			}	
		}
		dividedList.add(newList);
		return dividedList;
	}

	public static void printTimeStatistics(List<Long> runTime) {
		long totalTime = 0;
		long minTime = runTime.get(0);
		long maxTime = runTime.get(0);
		for (Long time : runTime) {
			totalTime += time;
			if(time > maxTime)
				maxTime = time;
			if(time < minTime)
				minTime = time;
		}

		System.out.println("\nAverage Time :: " + (float) totalTime / runTime.size());
		System.out.println("Minimum Time :: " + minTime);
		System.out.println("Maximum Time :: " + maxTime);

		System.out.println(runTime);
	}


	public static int fibonacci(int n) {
		if (n <= 1) 
			return n;
		else return fibonacci(n-1) + fibonacci(n-2);
	}

	public static void main(String[] args) {
		List<String> testList = new ArrayList<String>();
		for(int i = 0; i < 22; i++){
			testList.add(Integer.toString(i+1));
		}

		System.out.println(divideList(testList, 4));
	}
}

package com.WeatherData.com.WeatherData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class DataLoader {

	/*
	 * Loads the file contents to memory, reads each line and returns ArrayList of Strings 
	 * i/p : fileName
	 * o/p: ArrayList of Strings,   
	 */
	public static List<String> loadFromFile (String path) {

		List<String> fileContents = new ArrayList<String>();

		try(BufferedReader fileReader = new BufferedReader(new FileReader(path))){
			String line;
			while((line = fileReader.readLine()) != null){
				fileContents.add(line);
			}
			fileReader.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		return fileContents;
	}

	
	/*
	 * Reads a given file and distributes each line into given number of lists in round-robin fashion
	 * and returns a List of lists.
	 */
	public static List<List<String>> loadIntoMultipleList(String path, int numberOfList) {
		List<List<String>> dividedList = new ArrayList<List<String>>();

		for (int i = 0; i < numberOfList; i++) {
			dividedList.add(new ArrayList<String>());
		}

		try(BufferedReader fileReader = new BufferedReader(new FileReader(path))){
			String line;
			int turnCount = 0;
			while((line = fileReader.readLine()) != null){
				dividedList.get(turnCount).add(line);
				
				if(turnCount < numberOfList - 1)
					turnCount++;
				else
					turnCount = 0;
			}
			fileReader.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return dividedList;
	}
}

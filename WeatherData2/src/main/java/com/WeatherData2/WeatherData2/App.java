package com.WeatherData2.WeatherData2;

import java.io.File;

public class App 
{
	public static void main( String[] args )
	{
		if(args.length < 3){
			System.out.println("Input ERROR: Arguments Insufficient");
			return;
		}

		int choice = Integer.parseInt(args[2]); 

		try {

			switch(choice) {

			case 1 : 	WeatherAnalyzer1A.WeatherAnalyzer1ADriver(args);
			break;

			case 2 : 	WeatherAnalyzer1B.WeatherAnalyzer1BDriver(args);
			break;

			case 3 :	WeatherAnalyzer1C.WeatherAnalyzer1CDriver(args);	
			break;

			case 4 :	WeatherAnalyzer2.WeatherAnalyzer2Driver(args);
			break;

			default: 	System.out.println("ERROR in input, Version Number can be between 1-5");
			break;

			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	// used to delete directory while debugging on IDE 
	public static boolean deleteDir(File dir) 
	{ 
		if (dir.isDirectory()) 
		{ 
			String[] children = dir.list(); 
			for (int i=0; i<children.length; i++)
			{ 
				boolean success = deleteDir(new File(dir, children[i])); 
				if (!success) 
					return false; 
			} 
		}  
		// The directory is now empty and can be deleted
		// or it is a file. so delete it
		return dir.delete(); 
	} 
	
	public static boolean deleteFile(File file) 
	{ 
		if(file.exists())
			return file.delete();
		return file.delete();
	}
}

package com.WeatherData.com.WeatherData;

/*
 * Entry point to run a version of the program
 */
public class WeatherDataApp 
{
	/*
	 * Runs the version of the program as given in the argument,
	 * Returns if number of arguments are insufficient
	 * 
	 * input passed as arguments to String[] args: 
	 * 		Version(possible inputs) : 1,2,3,4,5 
	 * 		Number of Threads : 1,2,3,.....
	 * 		Input File: full path to the input file containing weather data 
	 */
	public static void main( String[] args )
	{
		if(args.length < 3){
			System.out.println("Input ERROR: Arguments Insufficient");
			return;
		}
		
		System.out.println("\nNOTE: All average results will be printed to 'out' folder");

		int choice = Integer.parseInt(args[0]);
		int threadCount = Integer.parseInt(args[1]);
		String inputPath = args[2]; 
		
		switch(choice) {

		case 1 : 	Seq seq = new Seq();
					seq.performTmaxComputation(inputPath);
					break;

		case 2 : 	NoLock nolock = new NoLock();
					nolock.performTmaxComputation(threadCount, inputPath);
					break;

		case 3 :	CoarseLock coarseLock = new CoarseLock();
					coarseLock.performTmaxComputation(threadCount, inputPath);
					break;

		case 4 :	FineLock fineLock = new FineLock();
					fineLock.performTmaxComputation(threadCount, inputPath);
					break;

		case 5 :	NoSharing noSharing = new NoSharing();
					noSharing.performTmaxComputation(threadCount, inputPath);
					break;
					
		default: 	System.out.println("ERROR in input, Version Number can be between 1-5");
					break;
		}
	}
}

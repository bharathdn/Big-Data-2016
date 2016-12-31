package Utils;

import BirdSightPrediction.PredictionDriver;
import BirdSightTraining.TrainingDriver;

public class App {
	
	public static void main(String[] args) throws Exception {
		
		if(args.length != 3) {
			System.out.println("Error: Invalid number of arguments.");
			return;
		}
		
		/*
		 * args[0] : Input path for Labelled data
		 * args[1] : Input path for UnLabelled data
		 * args[2] : Path for outputting data
		 */
		
		// Training
		String[] trainingArgs = new String[2];
		trainingArgs[0] = args[0];
		trainingArgs[1] = args[2] + "/trainingOutput";
		TrainingDriver.BirdSightTrainingDriver(trainingArgs);
		
		
		// Testing (Prediction)
		String[] testingArgs = new String[2];
		testingArgs[0] = args[1];
		testingArgs[1] = args[2] + "/testingOutput";
		PredictionDriver.BirdSightPredictionDriver(testingArgs);
	}

}

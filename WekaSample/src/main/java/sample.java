import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.sun.xml.bind.v2.runtime.unmarshaller.TextLoader;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayesUpdateable;
import weka.classifiers.evaluation.NominalPrediction;
import weka.classifiers.rules.DecisionTable;
import weka.classifiers.rules.PART;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.core.converters.CSVLoader;
import weka.core.converters.ConverterUtils.DataSource;

public class sample {
	
	public static BufferedReader readDataFile(String filename) {
		BufferedReader inputReader = null;
 
		try {
			inputReader = new BufferedReader(new FileReader(filename));
		} catch (FileNotFoundException ex) {
			System.err.println("File not found: " + filename);
		}
 
		return inputReader;
	}

	
	public static void main(String[] args) throws Exception {
		
		String train = "input/sample.csv";
//		BufferedReader datafile = readDataFile("input/sample.txt");
		
		DataSource source = new DataSource(train);
		Instances structure = source.getDataSet();
		structure.setClassIndex(structure.numAttributes() - 1);
		
		NaiveBayesUpdateable nvb = new NaiveBayesUpdateable();
		nvb.buildClassifier(structure);
		
//		System.out.println(nvb);
		
		String testFile = "input/test.csv";
		
		DataSource testSource = new DataSource(testFile);
		Instances testStructure = testSource.getDataSet();
		testStructure.setClassIndex(testStructure.numAttributes() - 1);
		
		Evaluation eval = new Evaluation(structure);
		eval.evaluateModel(nvb, testStructure);
		System.out.println(testStructure.numInstances());
		System.out.println();
		for (int i = 0; i < testStructure.numInstances(); i++) {
			double pred = nvb.classifyInstance(testStructure.instance(i));
			System.out.println((int)pred);
			System.out.println("|");
		}

		
//		ArffLoader loader = new ArffLoader();
//		loader.setFile(new File(input));
//		
//		Instances structure = loader.getStructure();
//		structure.setClassIndex(structure.numAttributes() - 1);
		
//		NaiveBayesUpdateable nvb = new NaiveBayesUpdateable();
//		nvb.buildClassifier(structure);
//		 
//		Instance current;
//		while ((current = loader.getNextInstance(structure)) != null) {
//			   nvb.updateClassifier(current);
//		}
//		
//		System.out.println(nvb);
	}
}

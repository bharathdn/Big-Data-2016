package BirdSightTraining;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.Constants;
import Utils.FileHelper;
import Utils.InstanceHelper;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;

public class TrainingReducer {
	
	public static class TrainerReducer 
	extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			ArrayList<Attribute> attributes = InstanceHelper.getAttributes();
			Instances trainingInstances = new Instances(Constants.TRAINING, attributes, 1);
			trainingInstances.setClassIndex(trainingInstances.numAttributes() - 1);
		
			for(Text features : values) {
				Instance instance = InstanceHelper.getInstance(features.toString());
				trainingInstances.add(instance);
			}
			
			// Build classifier with instances
			NaiveBayes nvbClassifier = new NaiveBayes();
			try {
				nvbClassifier.buildClassifier(trainingInstances);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try {
				FileHelper.writeModelToFile(nvbClassifier, context, key.toString());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}
		

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
		}
	}

}

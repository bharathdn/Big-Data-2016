package BirdSightTraining;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.Constants;
import Utils.Features;
import Utils.FileHelper;
import Utils.InstanceHelper;
import weka.classifiers.trees.RandomTree;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;

public class TrainingReducer {
	
	public static class TrainerReducer 
	extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			ArrayList<Attribute> attributes = InstanceHelper.getAttributes();
			Instances trainingInstances = new Instances(Constants.TRAINING, attributes, 0);
			trainingInstances.setClassIndex(trainingInstances.numAttributes() - 1);
		
			for(Text features : values) {
				Features featuresInstance = Features.getFeatures(features.toString());
				Instance instance = InstanceHelper.getInstance(trainingInstances, featuresInstance);
				trainingInstances.add(instance);
			}
			
			RandomTree randomTreeClassifier = new RandomTree();
			try {
				randomTreeClassifier.buildClassifier(trainingInstances);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try {
				FileHelper.writeClassifierToFile(randomTreeClassifier, context, key.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}

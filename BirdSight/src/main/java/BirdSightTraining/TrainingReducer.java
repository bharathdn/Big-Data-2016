package BirdSightTraining;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.Constants;
import Utils.Features;
import Utils.FileHelper;
import Utils.InstanceHelper;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Attribute;
import weka.core.DenseInstance;
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
				Features featuresInstance = Features.getFeatures(features.toString());
				Instance instance = getInstance(trainingInstances, featuresInstance);
				trainingInstances.add(instance);
			}
			
			NaiveBayes nvbClassifier = new NaiveBayes();
			try {
				nvbClassifier.buildClassifier(trainingInstances);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try {
				FileHelper.writeModelToFile(nvbClassifier, context, key.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		public static Instance getInstance(Instances instances, Features features) {
			Instance instance = new DenseInstance(Constants.FEATURE_COUNT);
			instance.setDataset(instances);
			
			instance.setValue(Constants.YEAR_INDEX, features.getYear());
			instance.setValue(Constants.MONTH_INDEX, features.getMonth());
			instance.setValue(Constants.DAY_INDEX, features.getDay());
			instance.setValue(Constants.TIME_INDEX, features.getTime());
			instance.setValue(Constants.COUNTRY_INDEX, features.getCountry());
			instance.setValue(Constants.STATE_PROVINCE_INDEX, features.getState());
			instance.setValue(Constants.COUNTY_INDEX, features.getCounty());
			instance.setValue(Constants.NUMBER_OBSERVERS_INDEX, features.getNumberOfObservers());
			instance.setValue(Constants.Agelaius_phoeniceus_INDEX, features.getAgelaiusPhoeniceus());
			
			return instance;
		} 
		
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}
		

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
		}
	}

}

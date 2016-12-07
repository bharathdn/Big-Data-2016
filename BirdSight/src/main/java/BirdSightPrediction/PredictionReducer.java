package BirdSightPrediction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
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

public class PredictionReducer {

	public static class PredictorReducer
	extends Reducer<Text, Text, Text, NullWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String, Integer> results = new HashMap<>();

			RandomTree classifier;
			try {
				classifier = (RandomTree) FileHelper.readClassifierFromFile(context, key.toString());

				ArrayList<Attribute> attributes = InstanceHelper.getAttributes();
				Instances testingInstances = new Instances(Constants.TRAINING, attributes, 0);
				testingInstances.setClassIndex(testingInstances.numAttributes() - 1);
				
				for(Text features : values) {
					Features featuresInstance = Features.getFeatures(features.toString());
					Instance instance = InstanceHelper.getInstance(testingInstances, featuresInstance);
					int prediction = (int) classifier.classifyInstance(instance);
					results.put(getEventId(features), prediction);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			for(Entry<String, Integer> entry : results.entrySet()) {
				String eventId = entry.getKey();
				String result = entry.getValue().toString();
				String resultToWrite = eventId + "," + result;
				context.write(new Text(resultToWrite), NullWritable.get());
			}
		}
		
		public static String getEventId(Text text) {
			String[] textSplit = text.toString().split(",");
			return textSplit[Constants.SAMPLING_EVENT_ID_INDEX].trim();
		}
	}
}

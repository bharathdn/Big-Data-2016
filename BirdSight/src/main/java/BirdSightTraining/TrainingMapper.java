package BirdSightTraining;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrainingMapper {
	
	public static class TrainerMapper 
	extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			String[] lineSplit = line.split(",");
			StringBuilder sb = new StringBuilder();
			for(int i = 0 ; i < 27 ; i++){
				sb.append(lineSplit[i]);
			}

			sb.deleteCharAt(sb.length() - 1);
			// 9 is column number for state in csv
			Text state = new Text(lineSplit[9]);
			context.write(state, new Text(sb.toString()));
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}


		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
		}
	}

}

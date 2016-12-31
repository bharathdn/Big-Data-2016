package BirdSightPrediction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import BirdSightTraining.TrainPredictMapper;
import BirdSightTraining.TrainingDriver;

public class PredictionDriver {

	public static void BirdSightPredictionDriver(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "BirdSight Testing");
		
//		FileHelper.deleteDir(new File(args[1]));

		job.setJarByClass(TrainingDriver.class);
		job.setMapperClass(TrainPredictMapper.TrainerMapper.class);
		job.setReducerClass(PredictionReducer.PredictorReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String args[]) throws Exception {
		BirdSightPredictionDriver(args);
	}

}

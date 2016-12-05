package parser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InitializePageRank {
	
	public static class  PageRankInitializerMapper 
	extends Mapper<Object, Text, Text, DoubleWritable> {
		
		private static long linkCount = 0;
		private static DoubleWritable initialPR = new DoubleWritable();
		private static Text DUMMY = new Text("!DUMMY");
		
		protected void setup(Context context) throws IOException, InterruptedException {
			linkCount = Long.valueOf(context.getConfiguration().get("LinkCount"));
			initialPR = new DoubleWritable((double) 1 / (double) linkCount);
		}


		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(DUMMY, initialPR);
		}
	}
	
	public static class PageRankInitializerReducer 
	extends Reducer<Text, DoubleWritable, LongWritable, DoubleWritable> {

		private static long linkCount = 0;
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Inside Reducer PR init");
			
			for(DoubleWritable value : values) {
				context.write(new LongWritable(linkCount++), value);
			}
		}	
	}
	
	public static void PageRankInitDriver(String[] args, long linkCount) 
			throws Exception { 

		Configuration conf = new Configuration();
		conf.set("LinkCount", Long.toString(linkCount));

		Job job = Job.getInstance(conf, "PageRankInitializerMR");
		
		job.setNumReduceTasks(1);
		
		job.setJarByClass(InitializePageRank.class);
		job.setMapperClass(PageRankInitializerMapper.class);
		job.setReducerClass(PageRankInitializerReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
	
	public static void main(String[] args, long linkCount) throws Exception {
//		Helper.deleteDir(new File(args[1]));
		PageRankInitDriver(args, linkCount);
	}
}


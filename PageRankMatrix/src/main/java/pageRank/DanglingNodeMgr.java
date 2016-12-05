package pageRank;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// class that has map reduce methods to add dangling pointer share to pageRanks
public class DanglingNodeMgr {

	// Global counter to set Dangling node page rank share
	static enum UpdateCount {
		DanglingNodePRShare
	}

	/*
	 * Mapper for Dangling 
	 */
	public static class DanglingNodeMapper 
	extends Mapper<Object, Text, Text, Text> {

		private static Text DUMMY = new Text("!DUMMY");

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// value => { danglingNodeId }
			Text danglingNodeId = new Text(value.toString().trim());
			context.write(DUMMY, danglingNodeId);
		}
	}


	public static class PageRankReducer 
	extends Reducer<Text, Text, Text, DoubleWritable> {

		Map<Long, Double> rankMap = new HashMap<Long, Double>();
		double danglingPrTotal = 0.0;
		private static long linkCount = 0;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			linkCount = Long.valueOf(context.getConfiguration().get("LinkCount"));

			Configuration conf = context.getConfiguration();
			URI[] uris = context.getCacheFiles();
			Path awsPath = new Path(uris[0]);
			FileSystem fs = FileSystem.get(awsPath.toUri(), conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(awsPath)));
			String line;
			while ((line = br.readLine()) != null) {
				if(line.length() > 0) {
					String[] lineSplit = line.split("\t");
					Long nodeId =  Long.parseLong(lineSplit[0]);
					Double pageRank =  Double.parseDouble(lineSplit[1]);
					rankMap.put(nodeId, pageRank); 
				}
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text nodeId : values) {
				Long nodeIdInt = Long.parseLong(nodeId.toString());
				if(rankMap.get(nodeIdInt) == null) {
					continue;
				}
				danglingPrTotal += rankMap.get(nodeIdInt);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// set the Dangling node pointer share
			double danglingPrShare = (double) danglingPrTotal / (double) linkCount;
			System.out.println("NEW DPR =====" + danglingPrShare);
			long newDanglingNodePRLong = (long) (danglingPrShare * 1000000000);
			context.getCounter(UpdateCount.DanglingNodePRShare).increment(newDanglingNodePRLong);
		}
	}


	public static double DanglingNodeDriver(String[] args, long linkCount) //, double iterationDanglingPr, long linkCount) 
			throws Exception { 

		Configuration conf = new Configuration();
		conf.set("LinkCount", Long.toString(linkCount));
		conf.set("DanglingPrShare", Long.toString(0l));

		Job job = Job.getInstance(conf, "PageRankMatrixDR");
		System.out.println("DRIVER1 ::CREATING ALIASED DANGLING");
		job.addCacheFile(new Path(args[2]).toUri());
		System.out.println("DRIVER2 ::CREATING ALIASED DANGLING");

		job.setJarByClass(PageRankMgr.class);
		job.setMapperClass(DanglingNodeMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1])); 
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		job.waitForCompletion(true);

		double dprShare = job.getCounters().findCounter(UpdateCount.DanglingNodePRShare).getValue(); 
		dprShare /= 1000000000;
		return dprShare;
	}

	public static double main(String[] args) throws Exception {
		// args[4] is ouput path, delete if it exists
		//		Helper.deleteDir(new File(args[4]));
		long linkCount = Long.parseLong(args[5]);
		return DanglingNodeDriver(args, linkCount);
	}

}

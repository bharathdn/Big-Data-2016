package pageRank;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PageRankMgr {

	/*
	 * Mapper for Matrix 
	 */
	public static class PageRankMatrixMapper 
	extends Mapper<Object, Text, Text, ReduceTuple> {

		private static BooleanWritable isRankValue = new BooleanWritable(false);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// value => { parentNodeId	adjList }
			// eg: { A	=> 	B,C,D
			String[] textSplit = value.toString().split("\t");

			LongWritable parentId = new LongWritable(Long.parseLong(textSplit[0]));
			List<Text> adjList = new ArrayList<Text>();
			if(textSplit.length > 1) {
				adjList = getAdjacentList(textSplit[1]);
			}
			
			DoubleWritable adjListSize = new DoubleWritable(adjList.size());
			ReduceTuple valueToEmit = new ReduceTuple(parentId, adjListSize, isRankValue);

			for(Text adjNode : adjList) {
				context.write(adjNode, valueToEmit);
			}
		}


		public static List<Text> getAdjacentList(String adjListStr) {
			String[] adjListArray = adjListStr.split("\\|");
			List<Text> adjList = new ArrayList<Text>();
			for(String nodeId : adjListArray) {
				adjList.add(new Text(nodeId));
			}
			return adjList;
		}  
	}


	public static class PageRankReducer 
	extends Reducer<Text, ReduceTuple, Text, DoubleWritable> {

		Map<Long, Double> rankMap = new HashMap<Long, Double>();
		private static double dprShare = 0.0;
		private static long linkCount = 0;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			dprShare = Double.valueOf(context.getConfiguration().get("DPR"));
			linkCount = Long.valueOf(context.getConfiguration().get("LinkCount"));

			System.out.println("dprShareObtained "+ dprShare);

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


		public void reduce(Text key, Iterable<ReduceTuple> values, Context context)
				throws IOException, InterruptedException {

			// nodeId to parentId mapping
			Map<Long, Long> nodeMap = new HashMap<Long, Long>();

			// add index and values to respective lists
			for (ReduceTuple tuple : values) {  
				nodeMap.put(tuple.getParentId().get(), (long)tuple.getValue().get());
			}

			double newRank = 0.0;
			for(Map.Entry<Long, Long> nodeSet : nodeMap.entrySet()) {
				if(rankMap.get(nodeSet.getKey()) != null) {
					newRank += (double)(1.0 / nodeSet.getValue()) * rankMap.get(nodeSet.getKey()) ;
				}
			}

			newRank += dprShare;
			newRank = (0.15 / (double)linkCount) + (0.85 * newRank);
			context.write(key, new DoubleWritable(newRank));
		}
	}


	public static void PageRankDriver(String[] args, long linkCount, double dprShare) 
			throws Exception { 

		Configuration conf = new Configuration();
		conf.set("LinkCount", Long.toString(linkCount));
		conf.set("DPR", Double.toString(dprShare));

		Job job = Job.getInstance(conf, "PageRankMatrixMR");
		job.addCacheFile(new Path(args[2]).toUri());

		job.setJarByClass(PageRankMgr.class);
		job.setMapperClass(PageRankMatrixMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ReduceTuple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ReduceTuple.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		job.waitForCompletion(true);
	}

	public static void main(String[] args, double dprShare) throws Exception {
		//		Helper.deleteDir(new File(args[3]));
		long linkCount = Long.parseLong(args[5]);
		PageRankDriver(args, linkCount, dprShare);
	}

}
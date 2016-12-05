package pageRankB;

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
import org.apache.hadoop.hdfs.server.datanode.dataNodeHome_jsp;
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
	extends Mapper<Object, Text, LongWritable, DoubleWritable> {

		Map<Long, Double> rankMap = new HashMap<Long, Double>();
		private static double dprShare = 0.0;
		private static long linkCount = 0;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			dprShare = Double.valueOf(context.getConfiguration().get("DPR"));
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

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// value => { parentNodeId	inlinks }
			// eg: { A	=> 	B,C,D
			String[] textSplit = value.toString().split("\t");

			
			LongWritable parentId = new LongWritable(Long.parseLong(textSplit[0]));
			List<Long> adjList = new ArrayList<Long>();
			
			Double newPr = 0.0;
			if(textSplit.length > 1) {
				adjList = getAdjacentList(textSplit[1]);
				for(Long inLink : adjList) {
					if(rankMap.get(inLink) != null) {
						newPr += rankMap.get(inLink);
					}
				}
			}
			
			newPr += dprShare;
			
			newPr = (0.15 / (double)linkCount) + (0.85 * newPr);
			context.write(parentId, new DoubleWritable(newPr));
		}


		public static List<Long> getAdjacentList(String nodes) {
			List<Long> adjList = new ArrayList<Long>();
			String[] outLinks = nodes.split("\\|");
			for (int i = 0; i < outLinks.length; i++) {
					adjList.add(Long.parseLong(outLinks[i]));
			}
			return adjList;
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

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

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
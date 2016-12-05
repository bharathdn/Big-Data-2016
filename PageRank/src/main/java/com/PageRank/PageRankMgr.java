package com.PageRank;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * Distributed PageRank Computationx
 * 
 * Class that contains map and reduce functions.
 * It contains methods that perform iterations of PageRank on given graph and 
 * outputs pagerank values for nodes after every iteration.
 */
public class PageRankMgr {
	
	// Global counter to set Dangling node page rank share
	static enum UpdateCount {
		DanglingNodePRShare
	}

	public static class PageRankMapper 
	extends Mapper<Object, Text, Text, Node> {
		private final static Text DUMMYKEY = new Text("!DUMMY");
		private static double DanglingNodePRShare = 0.0;
		private static long linkCount = 0;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			DanglingNodePRShare = Double.valueOf(context.getConfiguration().get("DPR"));
			linkCount = Long.valueOf(context.getConfiguration().get("LinkCount"));
		}

		/*
		 * map function takes a line from parser, further parses the line and outputs a new PageRank for given node
		 * 
		 * Special cases: Dangling nodes are handled by emitting them with a pre-chosen Dummy key
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String linkLine = value.toString();
			String[] links = linkLine.split("\t");

			double pr = 0.0;
			
			// this condition occurs only when, reading file from Parsers' output
			if(Double.valueOf(links[1]) == -1.0) {
				pr = (double) 1 / linkCount;
			}
			else {
				pr = Double.valueOf(links[1]); 
			}

			// add dangling nodes share from previous iteration
			pr += DanglingNodePRShare;
			DoubleWritable pageRankWritable = new DoubleWritable(pr);

			Text nodeId = new Text(links[0]);
			ArrayList<Text> adjList = new ArrayList<Text>();
			
			// Check if node contains adj list, or if it is a dangling node
			if(links.length > 2) { 
				String[] outLinks = links[2].split("\\|");
				for (int i = 0; i < outLinks.length; i++) {
					Text link = new Text(outLinks[i]);
					adjList.add(link);
				}
				context.write(nodeId, new Node(adjList, pageRankWritable));

				// emit PR share for adj List nodes
				if(adjList.size() > 0) {
					double prAdj = (double) pr / adjList.size(); 
					DoubleWritable pageRankForAdjNodes = new DoubleWritable(prAdj);
					for(Text nodeid : adjList) {
						context.write(nodeid, new Node( pageRankForAdjNodes));
					}
				}
			}
			else {
				//emit dummy key for Dangling nodes
				context.write(DUMMYKEY, new Node(pageRankWritable));
			}
		}
	}

	
	public static class PageRankReducer 
	extends Reducer<Text, Node, Text, Node> {
		private final static Text DUMMYKEY = new Text("!DUMMY");
		private static long linkCount = 0;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			linkCount = Long.valueOf(context.getConfiguration().get("LinkCount"));
		}
		
		/*
		 * Reduce function that aggregates the pagerank values for a given node
		 */
		public void reduce(Text key, Iterable<Node> values, Context context)
				throws IOException, InterruptedException {
			if(key.equals(DUMMYKEY)) {
				double danglingPR = 0.0;
				for (Node val : values) {
					danglingPR += val.pageRank.get();
				}

				// since a single reducer gets all the dangling nodes, 
				// it is fine to re assign the value here
				double newDanglingNodePR = (double) danglingPR / linkCount;
				
				long newDanglingNodePRLong = (long) (newDanglingNodePR * 1000000000);
				context.getCounter(UpdateCount.DanglingNodePRShare).increment(newDanglingNodePRLong);
			}
			else {
				double localPageRank = 0.0;
				Node node = new Node();
				for (Node val : values) {
					// if adjList size is zero, it is not a node as per the design of this solution
					if(val.adjList.size() == 0) {
						localPageRank += val.pageRank.get();
					}
					// else recover the graph
					else {
						node.adjList = val.adjList;
					}
				}
				node.pageRank = new DoubleWritable((0.15 / linkCount) + (0.85 * localPageRank));
				context.write(key, node);
			}
		}
	}

	/*
	 * Driver function to perform pagerank
	 */
	public static double PageRankDriver(String[] args, double iterationDanglingPr, long linkCount) throws Exception { 

		Configuration conf = new Configuration();
		conf.set("DPR", Double.toString(iterationDanglingPr));
		conf.set("LinkCount", Long.toString(linkCount));
		Job job = Job.getInstance(conf, "Page Rank");
		job.setJarByClass(PageRankMgr.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Node.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		// return dangling node pagerank share to the caller
		return job.getCounters().findCounter(UpdateCount.DanglingNodePRShare).getValue();
	}


	/*
	 * method that performs PageRank for given number of iterations and 
	 * returns path of last iterations output
	 */
	public static String iteratePageRankJobs(long linkCount, String path, int numberOfIterations) throws Exception {
		
		double iterationDanglingPr = 0.0;
		String[] args = new String[2];
		String outputPath = "";
		for(int i = 0; i < numberOfIterations; i ++) {
			args[0] = path + Integer.toString(i);
			args[1] = path + Integer.toString(i + 1);
			
			// fetch the dangling node pagerank
			iterationDanglingPr = PageRankDriver(args, iterationDanglingPr, linkCount);
			iterationDanglingPr /= 1000000000;
			
			outputPath = args[1];
		}
		return outputPath;
	}
	
	
	public static void performPageRank(String[] args) throws Exception {
		// Step1: parse files
		// step2: Run PageRank in iterstions
		// step3: Sort the pageRank output to get the pages' pageRank probabilities
		
		long linkCount = MRParser.WikiParserDriver(args[0], args[1]+"0");
		String inputForSort = iteratePageRankJobs(linkCount, args[1], Integer.parseInt(args[2]));
		PageRankSort.PageRankSortDriver(inputForSort, args[1]+"Sorted");		
	}
}
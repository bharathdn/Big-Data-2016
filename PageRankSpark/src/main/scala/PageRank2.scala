import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import WikiParser.Bz2WikiParser


object PageRank2 {

	def main( args : Array[String] ) {

		if(args.length != 4) {
			println("ERROR: Invalid number of arguments");
			return;
		}

		val splitter = "\t";
		val nodeSplitterRegex = "\\|";
		val nodeSplitter = "|";

		// create driver
		val conf = new SparkConf().setAppName("PageRankSpark").setMaster("local");
		val context = new SparkContext(conf);

		var inputPath = args(0);
		var outputPath = args(1);
		val topPageCount = args(2).toInt;
		val iterations = args(3).toInt;

		// Parse the input
		val parsedLines = context.textFile(inputPath)
				.map(line => Bz2WikiParser.parseLinks(line))
				.map(parsedLine => parsedLine.split(splitter));
		
		// Create the graph structure
		var graph = parsedLines
				.filter ( parsedSplits => parsedSplits.length >= 2)
				.map(linesplit => (linesplit(0), (-1.0, if(linesplit.length > 2) 
					(linesplit(2).split(nodeSplitterRegex).toList) 
					else List()))); //.persist();

		// Create the graph with adjacent lists, which will be used to join during a later stage
		var adjListGraph = parsedLines
				.filter { parsedSplits => parsedSplits.length > 2 }
		.map(linesplit => (linesplit(0), linesplit(2).split(nodeSplitterRegex).toList)); //.persist();

		val linkCount = graph.count();

		println("LinkCount = "+ linkCount);

		// Dangling pointer page-rank share
		var dprShare = 0.0;
		// isFirstIter helps in determining if the iteration should consider an intialPageRank value 
		// or if the pageRank value from previous iteration should be considered
		var isFirstIter = true;
		val initialPR = 1.toDouble / linkCount.toDouble;

		//	   Iteratively run PageRank 
		for( i <- 0 to iterations - 1) {
			println("\nRUNNING ITERATION :: " + i + "\n");

			// compute dpr: danglingNodePageRank
			val dpr = graph
			.filter{ case (nodeId, (pr, adjList)) => adjList.length == 0 }
			.map { case (nodeId, (pr, adjList)) => (if(isFirstIter) initialPR else pr.toDouble) }
			.reduce((dprSum, dpr) => dprSum + dpr);

			dprShare = dpr / linkCount.toDouble;
			
			println("DPRSHARE  == "+ dprShare);

			// Emit individual PR contirbutions
			val contributionNodes = graph
			.filter { case (nodeId, (pr, adjList)) => adjList.length > 0 }
			.flatMap { case (nodeId, (pr, adjList)) => 
			  adjList.map(adjNodeId => (adjNodeId, (if(isFirstIter) 
				                                      (initialPR / adjList.size.toDouble) else 
					                                    (pr / adjList.size.toDouble)))) };
			/*
			 * Reduce Phase
			 * sum up all the pr contributions and compute new pr values a 
			 */		  
			val reducedNodeContributions = contributionNodes
			.reduceByKey((x, y) => x + y)
			.map{ case(nodeId, pr) => (nodeId, ((0.15 / linkCount) + (0.85 * (pr + dprShare)))) };

			var allNodes = reducedNodeContributions.fullOuterJoin(adjListGraph);
			
			// code that does:
			//   check that all nodes have pagerank, if not, assign a pagerank to such nodes
			//   recreate the graph structure for next iteration
			graph = allNodes.map { 
			case (nodeId, (pr:Option[Double], adjList: Option[List[String]])) 
			=> (nodeId, (pr.getOrElse((0.15 / linkCount.toDouble) + (0.85 * dprShare)), (adjList.getOrElse(List())))) }
			.map{ case(nodeId, (pr, adjList)) => (nodeId, (pr.toDouble, adjList)) };
			
			isFirstIter = false;
		}	  
		
		// after Iterations sort and print to file
		val sortedMap = graph
		.sortBy(_._2._1, true, 0)
		.map { case (nodeId, (pr, adjList)) => (nodeId +"\t" +pr) };

		val top = context.parallelize(sortedMap.take(topPageCount));
		top.coalesce(1).saveAsTextFile(outputPath);

    context.stop();
	}
}
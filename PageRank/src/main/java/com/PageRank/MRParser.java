package com.PageRank;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.chainsaw.Main;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**  Map-Reduce version of the parser
 * Decompresses bz2 file and parses Wikipages on each line. */
public class MRParser {
	private static Pattern namePattern;
	private static Pattern namePattern2;
	private static Pattern linkPattern;
	private static int totalPageNameCount;
	private static double INITIAL_PAGE_RANK = -1.0;
	private static final String entitySeparator = "\t";
	private static final String nodeSeparator = "|";

	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~|\\?]+)$");
		namePattern2 = Pattern.compile("^([^?????]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~|?]+)\\.html$");
		totalPageNameCount = 0;
	}
	
	// global counter for number of links
	static enum Link {
		Count
	}

	public static class Bz2WikiParserMapper extends Mapper<Object, Text, Text, NullWritable>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			boolean flag = true;
			// Configure parser.
			SAXParserFactory spf = SAXParserFactory.newInstance();
			try {
				spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
				SAXParser saxParser = null;
				saxParser = spf.newSAXParser();

				XMLReader xmlReader = null;
				xmlReader = saxParser.getXMLReader();

				// Parser fills this list with linked page names.
				List<String> linkPageNames = new LinkedList<>();
				xmlReader.setContentHandler(new WikiParser(linkPageNames));
				String line = value.toString();

				// Each line formatted as (Wiki-page-name:Wiki-page-html).
				int delimLoc = line.indexOf(':');
				String pageName = line.substring(0, delimLoc);
				String html = line.substring(delimLoc + 1);
				Matcher matcher = namePattern.matcher(pageName);
				Matcher matcher2 = namePattern2.matcher(pageName);
				html = html.replace("&", "&amp;");

				if (!matcher.find() || !matcher2.find()) {
					flag = false;
				}

				// Parse page and fill list of linked pages.
				linkPageNames.clear();
				try {
					xmlReader.parse(new InputSource(new StringReader(html)));
				} catch (Exception e) {
					// Discard ill-formatted pages.
					System.out.println("Error reading ");
					flag = false;
				}
				if(flag) {
					StringBuilder sb = new StringBuilder();
					for (int i = 0 ; i < linkPageNames.size() ; i++){
						sb.append(linkPageNames.get(i)).append(nodeSeparator);
					}
					if(sb.length() > 0) {
						sb.deleteCharAt(sb.length() - 1);
					}
					// Occasionally print the page and its links.
//					if (Math.random() < .01f) {
//						System.out.println(pageName + " - " + linkPageNames);
//					}
					
//					context.write(new Text(pageName), new Text(INITIAL_PAGE_RANK+ entitySeparator + linkNames));
					context.write(getCustomKey(pageName, sb.toString()), NullWritable.get());
					context.getCounter(Link.Count).increment(1);
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static Text getCustomKey(String pageName, String links) {
		String customKey = pageName + entitySeparator + INITIAL_PAGE_RANK + entitySeparator + links;
		return new Text(customKey);
	}

	public static long WikiParserDriver(String pathToInputFile, String pathToOutputFile) throws IOException, InterruptedException, ClassNotFoundException{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "LinkParser");
		job.setJarByClass(Bz2WikiParser.class);
		job.setMapperClass(Bz2WikiParserMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(pathToInputFile));
		FileOutputFormat.setOutputPath(job, new Path(pathToOutputFile));
		job.waitForCompletion(true);
		
		return job.getCounters().findCounter(Link.Count).getValue();
	}

	public static int getTotalPageNameCount(){
		return totalPageNameCount;
	}

	/** Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler {
		/** List of linked pages; filled by parser. */
		private List<String> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
				// Beginning of bodyContent div element.
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}
				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) {
					linkPageNames.add(matcher.group(1));
				}
			} else if (count > 0) {
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// End of element inside bodyContent div.
				count--;
			}
		}
	}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		String inputPath = "input/wikipedia-simple-html.bz2";
		String outPutPath = "/Users/bharathdn/Documents/MRworkspace/PageRankSpark/output/pagerank0";
		WikiParserDriver(inputPath, outPutPath);
	}
}
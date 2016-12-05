package WikiParser;

import java.io.Serializable;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/** Sequential version of the parser 
 * Decompresses bz2 file and parses Wikipages on each line. */
public class Bz2WikiParser implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Pattern namePattern;
	private static Pattern linkPattern;
	//	private static long linkCount = 0;
	private static long danglingCount = 0;
	private static final double INITIALPR = -1.0;



	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}

	public static long getDanglingNodeCount() {
		return danglingCount;
	}

	// returns number of unique Links
	public static String parseLinks(String lineToParse) {
		StringBuilder sb = new StringBuilder();

		boolean flag = true;
		try {
			// Configure parser.
			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			SAXParser saxParser = spf.newSAXParser();
			XMLReader xmlReader = saxParser.getXMLReader();
			// Parser fills this list with linked page names.
			List<String> linkPageNames = new LinkedList<>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));

			// Each line formatted as (Wiki-page-name:Wiki-page-html).
			int delimLoc = lineToParse.indexOf(':');
			String pageName = lineToParse.substring(0, delimLoc);
			String html = lineToParse.substring(delimLoc + 1);
			html = html.replace("&", "&amp;");
			Matcher matcher = namePattern.matcher(pageName);
			
			if (!matcher.find()) {
				flag = false;
			}

			linkPageNames.clear();
			try {
				xmlReader.parse(new InputSource(new StringReader(html)));
			} catch (Exception e) {
				flag = false;
			}
			if(flag) {
				sb.append(pageName).append("\t").append(INITIALPR).append("\t").append(getListAsString(linkPageNames)).append("\n");
				sb.deleteCharAt(sb.length() - 1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	public static String getListAsString(List<String> list) {
		StringBuilder sb = new StringBuilder();
		for(String s : list) {
			sb.append(s).append("|");
		}
		if(list.size() > 0) { 
			sb.deleteCharAt(sb.length() - 1); 
		}

		return sb.toString();
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
}
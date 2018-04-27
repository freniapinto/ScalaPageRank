package com.cs6240.spark.pgrank;

import java.io.StringReader;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import scala.Serializable;


public class Parser implements Serializable{
	private static Pattern linkPattern;
	static {
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}

	public List<String> makeGraph(String line) {
		List<String> linkPageNames;
		XMLReader xmlReader;
		
		try {
			// Configure parser.
			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			SAXParser saxParser = spf.newSAXParser();
			xmlReader = saxParser.getXMLReader();
			
			// Parser fills this list with linked page names.
			linkPageNames = new LinkedList<>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));
			
	        // Each line formatted as (Wiki-page-name:Wiki-page-html).
			int delimLoc = line.indexOf(':');
			String pageName = line.substring(0, delimLoc);
			//String pageName = new String(Arrays.copyOf(line.getBytes(), delimLoc), "latin1");
			String html = line.substring(delimLoc + 1);
			html = html.replaceAll("&", "&amp;");
			
			linkPageNames.clear();
			
			try {
				xmlReader.parse(new InputSource(new StringReader(html)));
			} catch (Exception e) {
				// Discard ill-formatted pages.
				return new LinkedList<String>();
			}
			
			
			return new PreProcess(linkPageNames,pageName).createAdjList();
			
		}
		catch(Exception e) {
			e.printStackTrace();
			return new LinkedList<String>();
		}
	}


				
	
	/**
	 * Pre-processing the graph
	 */
	private static class PreProcess {
		private List<String> adj;
		private String page;
		private Map<String, List<String>> graph;
		
		public PreProcess(List<String> g, String p) {
			adj = g;
			page = p;
		}
		
		public PreProcess(Map<String, List<String>> adjList) {
			graph = adjList;
		}
		
		public List<String> createAdjList() {
			// Remove duplicates in the adjacency list
			Set<String> uniqueList = new HashSet<String>();
			uniqueList.addAll(adj);
			List<String> list = new LinkedList<String>(uniqueList);
			return list;
		}
		
		public Map<String, List<String>> createDanglingNodes(){
			List<String> keyList = Collections.list(Collections.enumeration(graph.keySet()));
			List<List<String>> valueList = Collections.list(Collections.enumeration(graph.values()));
			List<String> lst = valueList.stream()
			        .flatMap(x -> x.stream())
			        .collect(Collectors.toList());
			Set<String> uniqueList = new HashSet<String>();
			uniqueList.addAll(lst);
			for (String each: uniqueList) {
				if (!keyList.contains(each)) {
					graph.put(each, new LinkedList<String>());
				}
			}
			return graph;
		}
		
		
	}
	
	/**
	 * Pretty Prints the html read from bz2 file
	 * @author frenia
	 *
	 */
	private static class PrettyPrinter {
		private String content;
		
		public PrettyPrinter(String cont) {
			content = cont;
		}
		
		public void print() {
			Document doc = Jsoup.parseBodyFragment(content);
			System.out.println(doc.body().html());
		}
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

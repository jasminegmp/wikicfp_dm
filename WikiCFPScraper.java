//  JASMINE - This is here because I used Eclipse to debug
package wikicfp_crawler_pt1;

import java.net.*;
import java.io.*;

// JASMINE - Using JSoup library
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements; 

public class WikiCFPScraper {
	public static int DELAY = 7;
	public static void main(String[] args) {
	
		try {

			// JASMINE - I created a category string array to go through all four categories
			String category[] = new String[4];
			category[0] = "data mining";
			category[1] = "databases";
			category[2] = "machine learning";
			category[3] = "artificial intelligence";
			
			//create the output file
			File file = new File("wikicfp_crawl.txt");
			file.createNewFile();
			FileWriter writer = new FileWriter(file); 
			
			for (int category_loop = 0; category_loop < category.length; category_loop++)
			{
				int numOfPages = 20;
				
				//now start crawling the all 'numOfPages' pages
				for(int i = 1;i<=numOfPages;i++) {
					//Create the initial request to read the first page 
					//and get the number of total results
					String linkToScrape = "http://www.wikicfp.com/cfp/call?conference="+
									  URLEncoder.encode(category[category_loop], "UTF-8") +"&page=" + i;
					String content = getPageFromUrl(linkToScrape);	  
					
					// JASMINE - The bulk of my code starts here
					// I used JSoup in order to parse through the HTML string
				
					// 1. Used a Jsoup's Document class to save CONTENT string into DOC
					// Got information on how JSoup works from: https://jsoup.org/cookbook/
					Document doc = Jsoup.parse(content);
					
					/*
					2. I used Jsoup's Element's class' select method to find elements matching my select query and stores it
					into an Elements objects that I created for the conference acronym, name, and location 
					Got information on how to use class Element's select from these documentation: 
					https://jsoup.org/apidocs/org/jsoup/select/Selector.html
					https://jsoup.org/cookbook/extracting-data/selector-syntax	
					https://jsoup.org/cookbook/extracting-data/example-list-links
					*/	
					
					// All conf acronyms span 2 rows (attribute) and and the acronym text is embedded in <a href></a> tag
					Elements confAcro = doc.select("td[rowspan=2] > a[href]");
					
					// All conf names span 3 columns and name text is embedded in the <td></td> tag
					Elements confName = doc.select("td[colspan=3]"); 
					
					// All conf locations are within a <td="left"> tag, and it is the item in the first column of a row 
					// and doesn't span across more than one column like the conf names
					Elements confLoc = doc.select("td[align=left]:eq(1):not([colspan])");
					
					// Looping through Elements' objects confAcro, confName, confLoc in one for loop to write into a text file separated by tabs
					// For help on how to increment through multiple Elements in one for loop, I read this Stack Overflow question:
					// https://stackoverflow.com/questions/29816625/jsoup-iterate-over-elements-causes-duplicated-output?rq=1
					for (int j = 0; j < confAcro.size() && j < confName.size() && j < confLoc.size(); j++)
					{
						writer.write(confAcro.eq(j).text() + "\t" + confName.eq(j).text() + "\t" + confLoc.eq(j).text() + "\n");
						//System.out.println(confAcro.eq(j).text()); // Was using this during debugging
					}
					writer.flush();
					
					//IMPORTANT! Do not change the following:
					Thread.sleep(DELAY*1000); //rate-limit the queries
				} // END of page loop
			} // END of for category loop
			writer.close();
		// END of Try
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * Given a string URL returns a string with the page contents
	 * Adapted from example in 
	 * http://docs.oracle.com/javase/tutorial/networking/urls/readingWriting.html
	 * @param link
	 * @return
	 * @throws IOException
	 */
	public static String getPageFromUrl(String link) throws IOException {
		URL thePage = new URL(link);
		URLConnection yc = thePage.openConnection();
		BufferedReader in = new BufferedReader(new InputStreamReader(
									yc.getInputStream()));
		String inputLine;
		String output = "";
		while ((inputLine = in.readLine()) != null) {
			output += inputLine + "\n";
		}
		in.close();
		return output;
	}
	
	
	
	}





/**
 * The mapper class for step1 matrix multipilcation
 * @author Elham Buxton, University of Illinois at Springfield
 *
 */
import java.io.IOException;
import java.util.*;
import java.io.Reader;
import java.io.StringReader;

import org.apache.hadoop.mapreduce.*;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.apache.hadoop.io.*;
import java.util.Locale;

public class TFidFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Hashtable<String, Integer> map;
		
	public TFidFMapper()
	{
		map = new Hashtable<String, Integer>();
		
		LoadStopWords();
		
	}
	public String RemoveNonText(String rawString)
	{
		int charCount =0;
		for (int i = 0; i < rawString.length(); i++)
		{
			if (!Character.isLetter(rawString.charAt(i)) &&
					rawString.charAt(i) != '-')
			{
				return new String("");
			}
			if (rawString.charAt(i) != '-')
			{
				charCount++;
			}
		}
		if (charCount < rawString.length()-1)
		{
			//if we have more than 1 non alphabet, move on
			return new String("");
		}
		return rawString;
	}
	public void LoadStopWords()
	{
		String[] string ={"a", "about", "above", "above", "across", 
				"after", "afterwards", "again", "against", "all", "almost", 
				"alone", "along", "already", "also","although","always","am",
				"among", "amongst", "amoungst", "amount",  "an", "and", "another", 
				"any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", 
				"as",  "at", "back","be","became", "because","become","becomes", 
				"becoming", "been", "before", "beforehand", "behind", "being", 
				"below", "beside", "besides", "between", "beyond", "bill", "both", 
				"bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", 
				"could", "couldnt", "cry", "de", "describe", "detail", "do", "done",
				"down", "due", "during", "each", "eg", "eight", "either", "eleven",
				"else", "elsewhere", "empty", "enough", "etc", "even", "ever", 
				"every", "everyone", "everything", "everywhere", "except", "few", 
				"fifteen", "fify", "fill", "find", "fire", "first", "five", "for", 
				"former", "formerly", "forty", "found", "four", "from", "front", 
				"full", "further", "get", "give", "go", "had", "has", "hasnt", 
				"have", "he", "hence", "her", "here", "hereafter", "hereby", 
				"herein", "hereupon", "hers", "herself", "him", "himself", 
				"his", "how", "however", "hundred", "ie", "if", "in", "inc", 
				"indeed", "interest", "into", "is", "it", "its", "itself", 
				"keep", "last", "latter", "latterly", "least", "less", "ltd", 
				"made", "many", "may", "me", "meanwhile", "might", "mill", 
				"mine", "more", "moreover", "most", "mostly", "move", "much", 
				"must", "my", "myself", "name", "namely", "neither", "never", 
				"nevertheless", "next", "nine", "no", "nobody", "none", "noone", 
				"nor", "not", "nothing", "now", "nowhere", "of", "off", "often", 
				"on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", 
				"our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", 
				"please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", 
				"seems", "serious", "several", "she", "should", "show", "side", "since", 
				"sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", 
				"sometime", "sometimes", "somewhere", "still", "such", "system", "take", 
				"ten", "than", "that", "the", "their", "them", "themselves", "then", 
				"thence", "there", "thereafter", "thereby", "therefore", "therein", 
				"thereupon", "these", "they", "thickv", "thin", "third", "this", "those", 
				"though", "three", "through", "throughout", "thru", "thus", "to", "together", 
				"too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", 
				"until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", 
				"what", "whatever", "when", "whence", "whenever", "where", "whereafter", 
				"whereas", "whereby", "wherein", "whereupon", "wherever", "whether", 
				"which", "while", "whither", "who", "whoever", "whole", "whom", "whose", 
				"why", "will", "with", "within", "without", "would", "yet", "you", "your", 
				"yours", "yourself", "yourselves", "the", "no"};
		
		for(int i=0; i < string.length; i++)
		{
			//all stopwords are already lower case
			map.put(string[i], new Integer(i));
		}		
	}
	
	public boolean IsStopWord(String term)
	{
		String lowerCase = term.toLowerCase(Locale.ENGLISH);
		
		Integer n = map.get(lowerCase);
		if (n != null)
		{
			return true;
		}
		return false;
	}
	public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
		String line = value.toString();
	    //retrieving the filename for which the input record comes from: orders.txt or customers.txt
	    //String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	    SAXBuilder builder = new SAXBuilder();
	    Reader read = new StringReader(line);
	    try {
	    	Document doc = builder.build(read);
	    	Element root = doc.getRootElement();
	    	String title = root.getChildText("title");
	    	String text = root.getChild("revision").getChildText("text");	    	
	    	
	    	String[] words = text.split("[\\s,*|& <>#%:()\\]\\[]");
	    	
	    	for (int i=0; i <words.length; i++)
	    	{
	    		String term = RemoveNonText(words[i]);
	    		if (term.length() == 0)
	    		{
	    			continue;
	    		}
	    		try {
	    			Integer.parseInt(term);
	    			//if this is a number, ignore it.
	    			continue;
	    		}
	    		catch (Exception e)
	    		{
	    			
	    		}
	    		if (IsStopWord(term))
	    		{
	    			continue;
	    		}
	    		if (term.length() > 0 && 
	    				(term.length() > 1 || Character.isLetter(term.charAt(0))))
	    		{
	    			context.write(new Text( term + "," + title), new IntWritable(1));
	    		}
	    	}
	    }
	    catch(JDOMException jx)
	    {
	    	System.out.print("jdom exception");
	    }
	    catch (Exception e)
	    {
	    	System.out.print(e.toString());
	    
	    }
  }
}

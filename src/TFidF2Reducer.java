import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.lang.Math;
import java.util.Iterator;
import java.util.ArrayList;
//import org.apache.hadoop.io.WritableUtils;

public class TFidF2Reducer extends
    Reducer<Text, Text, Text, DoubleWritable> {
	final int N = 16050432;
   private String getDocumentTF(Text text)
   {
	   String stringValue = text.toString();
		int tfStart = 0;
	    int tfEnd = stringValue.indexOf("}", tfStart);
	    String tf = stringValue.substring(tfStart, tfEnd);
	    
	    return tf;
   }
  /**
   * The reduce function simply forwards the key and emits the summation of values
   */
	public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException 
	{
	    
		int numDocs = 0;
		Iterator<Text> it = values.iterator();
		ArrayList<Text> string = new ArrayList<Text>();
		int totalTF = 0;
		
		while (it.hasNext())
	    {
	        Text ob = it.next();
	        string.add(new Text(ob.toString()));
			numDocs += 1;
			int docTF = Integer.parseInt(getDocumentTF(ob));
			totalTF += docTF;
	    }
		
	    for(int i =0; i < string.size(); i++)
	    {
	      String stringValue = string.get(i).toString();
	      
	      String tf = getDocumentTF(string.get(i));
	      try {
		      int indDocStart = stringValue.indexOf("}");
		      int indDocEnd = stringValue.indexOf("}", indDocStart+1);
	    	  String doc = stringValue.substring(indDocStart+1, indDocEnd);
		      double idf = Math.log(N/numDocs);
		      
		      context.write(new Text(key.toString() + ","+ doc+ "," + tf + ","+Integer.toString(totalTF)), new DoubleWritable(totalTF*idf));
	      }
	      catch (Exception e)
	      {
	    	  System.out.print(e.toString());
	      
	      }
	    }
	    
	}
}

//***************************************************************
//
//  Developer:    Marshal Pfluger
//
//  Project #:    Project Five
//
//  File Name:    Project5Reducer.java
//
//  Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//  Due Date:     10/13/2023
//
//  Instructor:   Prof. Fred Kumi 
//
//  Description:  This is a data analysis advertising program
//
//***************************************************************

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Ecommerce, Austin  [{39, 13} {281,5}.... etc] 
public class Project5Reducer extends Reducer<Text, Text, Text, Text>
{
	//***************************************************************
    //
    //  Method:       reduce (Override)
    // 
    //  Description:  Overrides the class inherited from Reducer
    //
    //  Parameters:   Text key, Iterable<Text> values, Context context
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
    {
    	// Define iterator to step through input
	    Iterator<Text> valuesIterator = values.iterator();
	    // Variable to hold result
	    String successRate = "";
	    // Variable to hold intermediate calculations 
	    double accumulator = 0.0;
	    // Counter to hold num adverts ran for each catgory and location
	    int numAdverts = 0;
		int numClicks = 0;
		int numSales = 0;
	    // Iterate through input
	    while (valuesIterator.hasNext()) {	
	    	// Unbox hadoop type to java 
	        String value = valuesIterator.next().toString();  // value   {clicks, sales}.
	        // Split on commas and ad to static array
	        String [] words = value.split(",");     // words  [{clicks} {sales}]
	        try {
		        // Parse number of clicks and sales 
				numClicks = Integer.parseInt(words[0]);
				numSales = Integer.parseInt(words[1]);
	        }
	        catch(NumberFormatException e) {
	        	e.printStackTrace();
	    		System.exit(1);
	        }
	        // Add the percentage of each add to running total
			accumulator += ((double) numSales / numClicks) * 100;
			// Increment num adverts
			numAdverts++;
	    } // End of while loop
	    // Format output to write
	    successRate = String.format("%.2f%%", (accumulator / numAdverts));
	    try {
	    	context.write(key, new Text(successRate));
	    	} 
	    catch (IOException e1) {
	    		e1.printStackTrace();
	    		System.exit(1);
			} 
	    catch (InterruptedException e1) {
				e1.printStackTrace();
				System.exit(1);
			}
	    }
    }
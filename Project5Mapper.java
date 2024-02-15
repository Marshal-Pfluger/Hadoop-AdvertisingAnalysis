//***************************************************************
//
//  Developer:    Marshal Pfluger
//
//  Project #:    Project Five
//
//  File Name:    Project5Mapper.java
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
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Project5Mapper extends Mapper<LongWritable, Text, Text, Text>
	{
	
	//***************************************************************
    //
    //  Method:       map (Override)
    // 
    //  Description:  Overrides the class inherited from Mapper
    //
    //  Parameters:   LongWritable key, Text value, Context con
    //
    //  Returns:      N/A 
    //
    //**************************************************************
		@Override
		public void map(LongWritable key, Text value, Context con)
		{
			// Unbox the hadoop data type to a java string
			String line = value.toString();
			// Seperate the commas and allow multiple spaces. 
			String[] words = line.split(",");
			// Set the value as the clicks and the sales
			Text outputValue = new Text(words[4] + "," + words[5]);
			// Set the key as the location and category
		    Text outputKey = new Text(words[3] + "," + words[2]);
			try {
				    // Write out the key/value pair
					con.write(outputKey, outputValue);
				}
			catch(IOException | InterruptedException e) {
				System.err.println(e);
				System.exit(1);
			}
		}
	}
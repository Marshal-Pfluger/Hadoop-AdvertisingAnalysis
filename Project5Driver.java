//***************************************************************
//
//  Developer:    Marshal Pfluger
//
//  Project #:    Project Five
//
//  File Name:    Project5Driver.java
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Project5Driver {

	//***************************************************************
    //
    //  Method:       main
    // 
    //  Description:  The main method of the program
    //
    //  Parameters:   String array
    //
    //  Returns:      N/A 
    //
    //**************************************************************
	public static void main(String[] args){
		//Instantiate object of class to call non static methods
		Project5Driver obj = new Project5Driver();
		//Call developer info method
		obj.developerInfo();
		//Call runDemo method
		obj.runDemo(args);
	}
	
	//***************************************************************
    //
    //  Method:       runDemo
    // 
    //  Description:  Runs the program in a non static method
    //
    //  Parameters:   String array
    //
    //  Returns:      N/A 
    //
    //**************************************************************
	public void runDemo(String[] args) {
	//If the input path and output path are not included exit program
	if (args.length != 2)
	{
	   System.err.println("Usage: WordCount <input path> <output path>");
	   System.exit(-1);
	}
	//Start try/catch block
	try {
		Configuration conf = new Configuration();
		String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
	
		Job job = Job.getInstance(conf, "Advertising");
		job.setJarByClass(Project5Driver.class);
		
		//Set Mapper, combiner, and Reducer classes
		job.setMapperClass(Project5Mapper.class);
		job.setReducerClass(Project5Reducer.class);
		
		//Output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
        // Submit Job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}catch(Exception e) {
		System.err.println(e);
		System.exit(1);
		}
	// Or written expanded
	// boolean status = job.waitForCompletion(true);
	// if (status)
	//    System.exit(0);
	// else
	//    System.exit(1);
	}
	
    //***************************************************************
    //
    //  Method:       developerInfo (Non Static)
    // 
    //  Description:  The developer information method of the program
    //                This method must be included in all projects.
    //
    //  Parameters:   None
    //
    //  Returns:      N/A
    //
    //***************************************************************
	public void developerInfo()
	{
		System.out.println("Name:    Marshal Pfluger");
		System.out.println("Course:  COSC 3365 Distributed Databases Using Hadoop");
		System.out.println("Program: Five\n");
		} // End of the developerInfo method
}
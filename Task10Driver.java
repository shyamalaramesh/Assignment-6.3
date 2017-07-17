package mapreduce.task10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Task10Driver {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		// initialize the configuration
		Configuration conf = new Configuration();
		
		// create a job object from the configuration and give it any name you want 
		Job job = Job.getInstance(conf, "Assignment_6.3 -> Task_10 -> "
									+ "Top 3 states for each company");
		
		// java.lang.Class object of the driver class
		job.setJarByClass(Task10Driver.class);

		// map function outputs key-value pairs. 
		// What is the type of the key in the output 
		// We want to find the top 3 sales for each company
		// So, it makes sense that we chose the company name as the 
		// key output by the mapper. So, the mapper output key type 
		// is Text 
		job.setMapOutputKeyClass(Text.class);
		// map function outputs key-value pairs. 
		// What is the type of the value in the output
		// In this case the value needs to be a combination
		// of state where the sales was done and the price of 
		// the sales. So, a class called SalesUnit is created to 
		// store both state and the price. This, of course implements 
		// the WritableComparable 
		job.setMapOutputValueClass(SalesUnit.class);
		
		// reduce function outputs key-value pairs. 
		// What is the type of the key in the output. 
		// In this case output is the top 3 states for 
		// each company. So, the key has to be the company name.
		// Therefore the key type is Text
		job.setOutputKeyClass(Text.class);
		// reduce function outputs key-value pairs. 
		// What is the type of the value in the output. 
		// In this case output is the top 3 states for 
		// each company. To store the state and the sales data, 
		// SalesUnit class is created. SalesUnit is a WritableCoparable 
		job.setOutputValueClass(SalesUnit.class);
		
		// Mapper class which has implemenation for the map phase
		job.setMapperClass(Task10Mapper.class);
		
		// Reducer class which has implementation for the reduce phase
		job.setReducerClass(Task10Reducer.class);
		
		// number of reduce tasks
		job.setNumReduceTasks(4);
		
		// Input is a text file. Hence TextInputFormat
		job.setInputFormatClass(TextInputFormat.class);
		
		 
		//Output file is a simple text file. So, output format is TextOutputFormat
		
		job.setOutputFormatClass(TextOutputFormat.class);

		/*
		 * The input path to the job. The map task will
		 * read the files in this path form HDFS 
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		/*
		 * The output path from the job. The map/reduce task will
		 * write the files to this path to HDFS. In this case the 
		 * reduce task will write to output path because number of 
		 * reducer tasks is not explicitly configured to be zero  
		 */
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.waitForCompletion(true);

	}
}

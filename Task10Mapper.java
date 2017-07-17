package mapreduce.task10;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task10Mapper extends Mapper<LongWritable, Text, Text, SalesUnit>{
	public static final String NA_STRING = "NA";
	/**
	 * We have to calculate the Mapper the top 3 states for each company. 
	 * The job has input format as TextInputFormat. So, the input key type is LongWritable(offset of the record from the file) 
	 * and the input value type is Text(content of the actual records in the file). So, the mapper class has output key type 
	 * as Text (company name) and the output value type should be a combination of state and price. So, a SalesUnit object is 
	 * used. SalesUnit is a WritableComparable. Also in the mapper invalid records are filtered.   
	 */
	public void map(LongWritable offsetInFile, Text recordInput, Context context) 
			throws IOException, InterruptedException {
		
		String[] recordFields = recordInput.toString().split("\\|");
		String companyName = recordFields[0].trim();
		String productName = recordFields[1].trim();
		if(!NA_STRING.equalsIgnoreCase(companyName) && 
				!NA_STRING.equalsIgnoreCase(productName) ) {
			SalesUnit salesUnit = new SalesUnit();
			salesUnit.set(recordFields[3], Integer.parseInt(recordFields[5]));
			context.write(new Text(companyName), salesUnit);
		}
	} 
}

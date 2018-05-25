package mapreduce.task.assignment;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class TaskMapper extends Mapper<LongWritable, Text,LongWritable,Text> {
		
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
		
		String company = lineArray[0];
		String product = lineArray[1];
		//Checking if company name or product name must not equal to NA
		if(!(company.equals("NA")||product.equals("NA")))
		{
			context.write(key,value);
		}
		
	}
}

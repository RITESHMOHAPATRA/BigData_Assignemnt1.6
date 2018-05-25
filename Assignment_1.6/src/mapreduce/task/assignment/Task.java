package mapreduce.task.assignment;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Task {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Assignmnet1.6");
		job.setJarByClass(Task.class);
                //Key is LongWritable as it is the index
		job.setMapOutputKeyClass(LongWritable.class);
		//Value is Text as the whole string is required
		job.setMapOutputValueClass(Text.class);
                
                //Key is LongWritable as it is the index
		job.setOutputKeyClass(LongWritable.class);
		//Value is Text as the whole string is required
		job.setOutputValueClass(Text.class);
		job.setMapperClass(TaskMapper.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		/*
		Path out=new Path(args[1]);
		out.getFileSystem(conf).delete(out);
		*/
		
		job.waitForCompletion(true);
	}
}

package Habits;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HourSkip {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"HourSkip"); 
		job.setJarByClass(HourSkip.class);
		
		job.setMapperClass(MyMapper.class);  
		job.setReducerClass(MyReducer.class);
		

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/StackOverflow/resource/生活习惯.txt"));
		Path outputPath = new Path("/StackOverflow/result/HourSkip");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isSuccessful=job.waitForCompletion(true);
		System.exit(isSuccessful?0:1);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text,IntWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {			
			Text text=new Text();
			text.set(value.toString().split("\t+")[2]);
			String word=value.toString().split("\t+")[3];
			IntWritable flag=new IntWritable();
			flag.set(-1);
			if ("3 - 4 times per week".equals(word)||"Daily or almost every day".equals(word)) {
				flag.set(1);
			}
			else if("1 - 2 times per week".equals(word)||"Never".equals(word)){
				flag.set(0);
			}
			context.write(text, flag);
		}
	}
	
	public static class MyReducer extends Reducer<Text,IntWritable, Text,NullWritable>{
		private static int hour0=0,hour1=0,hour2=0;
		private static double skip0=0,skip1=0,skip2=0;
		@Override
		protected void reduce(Text text, Iterable<IntWritable> arg1,Context context) 
				throws IOException, InterruptedException {
			if ("5 - 8 hours".equals(text.toString())) {
				for (IntWritable intWritable : arg1) {
					if (intWritable.get()==0) {
						hour1++;
					}
					else if (intWritable.get()==1) {
						hour1++;
						skip1++;
					}
				}
			}
			else if ("9 - 12 hours".equals(text.toString())||"Over 12 hours".equals(text.toString())) {
				for (IntWritable intWritable : arg1) {
					if (intWritable.get()==0) {
						hour2++;
					}
					else if (intWritable.get()==1) {
						hour2++;
						skip2++;
					}
				}
			}
			else if ("1 - 4 hours".equals(text.toString())||"Less than 1 hour".equals(text.toString())) {
				for (IntWritable intWritable : arg1) {
					if (intWritable.get()==0) {
						hour0++;
					}
					else if (intWritable.get()==1) {
						hour0++;
						skip0++;
					}
				}
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Text text2 =new Text();
			text2.set(String.format("less than 5 hours: %.2f%%\n5~8 hours: %.2f%%\nmore than 8 hours: %.2f%%", 
													skip0/hour0,skip1/hour1,skip2/hour2));
			context.write(text2, NullWritable.get());
		}
		
	}
	
	
}

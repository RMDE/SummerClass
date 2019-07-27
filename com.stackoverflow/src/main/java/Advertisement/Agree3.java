package Advertisement;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Agree3 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"Agree3"); 
		job.setJarByClass(Agree3.class);
		job.setMapperClass(MyMapper.class); 
		job.setReducerClass(MyReducer.class);
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/StackOverflow/resource/广告观.txt"));
		Path outputPath = new Path("/StackOverflow/result/Agree3");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isSuccessful=job.waitForCompletion(true);

		System.exit(isSuccessful?0:1);
	}
	
	public static class MyMapper extends Mapper<Text, Text, Text,IntWritable>{
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			IntWritable aIntWritable=new IntWritable(1);
			Text text=new Text();
			text.set(value.toString().split("\t+")[2]);
			context.write(text,aIntWritable);
		}
	}
	
	public static class MyReducer extends Reducer<Text,IntWritable, Text,NullWritable>{
		private static int sum0=0,sumy0=0,sumn0=0,sumy1=0,sumn1=0;
		private static float sum=0F;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) 
				throws IOException, InterruptedException {
			if("Neither agree nor disagree".equals(key.toString()))
			{
				for (IntWritable intWritable : values) {
					sum+=1;
					sum0++;
				}
			}
			else if("Somewhat agree".equals(key.toString()))
			{
				for (IntWritable intWritable : values) {
					sum+=1;
					sumy0++;
				}
			}
			else if ("Somewhat disagree".equals(key.toString())) {
				for (IntWritable intWritable : values) {
					sum+=1;
					sumn0++;
				}
			}
			else if("Strongly agree".equals(key.toString()))
			{
				for (IntWritable intWritable : values) {
					sum+=1;
					sumy1++;
				}
			}
			else if("Strongly disagree".equals(key.toString()))
			{
				for (IntWritable intWritable : values) {
					sum+=1;
					sumn1++;
				}
			}
		}
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			Text text=new Text();
			text.set(String.format("Strongly agree: %.2f%%\nSomewhat agree: %.2f%%\n"
					+ "Neither agree nor disagree: %.2f%%\nSomewhat disagree: %.2f%%\nStrongly disagree: %.2f%%", 
						sumy1/sum*100,sumy0/sum*100,sum0/sum*100,sumn0/sum*100,sumn1/sum*100));
			context.write(text, NullWritable.get());
		}
	}
}

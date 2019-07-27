package Ethics;

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

public class Choice {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"Choice"); 
		job.setJarByClass(Choice.class);
		job.setMapperClass(MyMapper.class); 
		job.setReducerClass(MyReducer.class);
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/StackOverflow/resource/道德层面.txt"));
		Path outputPath = new Path("/StackOverflow/result/Choice");
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
			context.write(key,aIntWritable);
		}
	}
	
	public static class MyReducer extends Reducer<Text,IntWritable, Text,NullWritable>{
		private static int sum0=0,sumy=0,sumn=0;
		private static float sum=0F;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) 
				throws IOException, InterruptedException {
			if("Depends on what it is".equals(key.toString()))
			{
				for (IntWritable intWritable : values) {
					sum+=1;
					sum0++;
				}
			}
			else if("Yes".equals(key.toString()))
			{
				for (IntWritable intWritable : values) {
					sum+=1;
					sumy++;
				}
			}
			else if ("No".equals(key.toString())) {
				for (IntWritable intWritable : values) {
					sum+=1;
					sumn++;
				}
			}
		}
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			Text text=new Text();
			text.set(String.format("Depends on what it is: %.2f%%\nYes: %.2f%%\nNo: %.2f%%", sum0/sum*100,sumy/sum*100,sumn/sum*100));
			context.write(text, NullWritable.get());
		}
	}
}

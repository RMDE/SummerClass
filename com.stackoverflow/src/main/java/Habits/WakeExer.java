package Habits;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WakeExer {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"WakeTime");  
		job.setJarByClass(WakeExer.class);
		
		job.setMapperClass(MyMapper.class); 
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/StackOverflow/resource/生活习惯.txt"));
		Path outputPath = new Path("/StackOverflow/result/WakeExer");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isSuccessful=job.waitForCompletion(true);
		System.exit(isSuccessful?0:1);
	}
	
	public static class MyMapper extends Mapper<Text, Text, Text,FloatWritable>{
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String word=value.toString().split("\t+")[0];
			float frequency=0;
			if ("1 - 2 times per week".equals(word)) {
				frequency=1.5F;
			}
			else if ("3 - 4 times per week".equals(word)) {
				frequency=3.5F;
			}
			else if ("Daily or almost every day".equals(word)) {
				frequency=7F;
			}
			FloatWritable sport=new FloatWritable();
			sport.set(frequency);
			context.write(key, sport);
		}
	}
	
	public static class MyReducer extends Reducer<Text,FloatWritable, Text,NullWritable>{
		private static int time_7=0,time7_9=0,time9_=0;
		private static double freq_7=0,freq7_9=0,freq9_=0;
		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values,Context context)
				throws IOException, InterruptedException {
			for (FloatWritable value : values) {
				if ("Between 5:00 - 6:00 AM".equals(key.toString())||
						"Between 6:01 - 7:00 AM".equals(key.toString())||"Before 5:00 AM".equals(key.toString())) {
					time_7++;
					freq_7+=value.get();
				}
				else if ("Between 7:01 - 8:00 AM".equals(key.toString())||"Between 8:01 - 9:00 AM".equals(key.toString())) {
					time7_9++;
					freq7_9+=value.get();
				}
				else if ("Between 9:01 - 10:00 AM".equals(key.toString())||"Between 10:01 - 11:00 AM".equals(key.toString())||
						"Between 11:01 AM - 12:00 PM".equals(key.toString())||"After 12:01 PM".equals(key.toString())) {
					time9_++;
					freq9_+=value.get();
				}
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			double frequency1=freq_7/time_7;
			double frequency2=freq7_9/time7_9;
			double frequency3=freq9_/time9_;
			Text text=new Text();
			text.set(String.format("before 7 clock: %.2f\n7~9 clock: %.2f\nafter 9 clock: %.2f", frequency1,frequency2,frequency3));
			context.write(text, NullWritable.get());
		}
	}
}

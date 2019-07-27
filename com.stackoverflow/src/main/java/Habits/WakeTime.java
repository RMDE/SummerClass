package Habits;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WakeTime {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"WakeTime");  //为该job命名，名字无影响。一般用类名
		job.setJarByClass(WakeTime.class);
		
		job.setMapperClass(MyMapper.class);  //此处代表设置mapper类
		job.setReducerClass(MyReducer.class);
		
		//指定mapper的输出类型，若mapper的输出类型和reduce的输出类型相同，则可以省略
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/StackOverflow/resource/生活习惯.txt"));
		Path outputPath = new Path("/StackOverflow/result/WakeTime");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isSuccessful=job.waitForCompletion(true);
		//HDFSUtil hdfsUtil =new HDFSUtil(conf);
		//hdfsUtil.showResultIn(outputPath.toString());
		System.exit(isSuccessful?0:1);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text,NullWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}
	}
	
	public static class MyReducer extends Reducer<Text,NullWritable, Text,NullWritable>{
		private static int time_5=0,time5_6=0,time6_7=0,time7_8=0,time8_9=0,time9_10=0,time10_=0;
		@Override
		protected void reduce(Text text, Iterable<NullWritable> arg1,Context context) 
				throws IOException, InterruptedException {
			String[] word=text.toString().split("\t");
			for (NullWritable nullWritable : arg1) {
				if (word[0].equals("Before 5:00 AM")||word[0].equals("I work night shifts")) {
					time_5++;
				}
				else if ("Between 5:00 - 6:00 AM".equals(word[0])) {
					time5_6++;
				}
				else if ("Between 6:01 - 7:00 AM".equals(word[0])) {
					time6_7++;
				}
				else if ("Between 7:01 - 8:00 AM".equals(word[0])) {
					time7_8++;
				}
				else if ("Between 8:01 - 9:00 AM".equals(word[0])) {
					time8_9++;
				}
				else if ("Between 9:01 - 10:00 AM".equals(word[0])) {
					time9_10++;
				}
				else if ("Between 10:01 - 11:00 AM".equals(word[0])
						||"Between 11:01 AM - 12:00 PM".equals(word[0])||"After 12:01 PM".equals(word[0])) {
					time10_++;
				}
			}
		}
		@Override
		protected void cleanup(Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			Text text = new Text();
			text.set("before 5 clock & all night: "+time_5+"\n5~6 clock: "+time5_6+"\n6~7 clock: "+time6_7+
						"\n7~8 clock: "+time7_8+"\n8~9 clock: "+time8_9+"\n9~10 clock: "+time9_10+"\nafter 10 clock: "+time10_);
			context.write(text, NullWritable.get());
		}
	}
}

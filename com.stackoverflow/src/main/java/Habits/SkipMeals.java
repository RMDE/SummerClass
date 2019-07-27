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


public class SkipMeals {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"SkipMeals");  //为该job命名，名字无影响。一般用类名
		job.setJarByClass(SkipMeals.class);
		
		job.setMapperClass(MyMapper.class);  //此处代表设置mapper类
		job.setReducerClass(MyReducer.class);
		
		//指定mapper的输出类型，若mapper的输出类型和reduce的输出类型相同，则可以省略
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/StackOverflow/resource/生活习惯.txt"));
		Path outputPath = new Path("/StackOverflow/result/SkipMeals");
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
			Text text=new Text();
			text.set(value.toString().split("\t+")[3]);
			context.write(text, NullWritable.get());
		}
	}
	
	public static class MyReducer extends Reducer<Text,NullWritable, Text,NullWritable>{
		private static int skip0=0,skip1=0,skip3=0,skip7=0;
		private static double sum=0;
		@Override
		protected void reduce(Text text, Iterable<NullWritable> arg1,Context context) 
				throws IOException, InterruptedException {
			if ("Never".equals(text.toString())) {
				for (NullWritable nullWritable : arg1) {
					skip0++;
					sum++;
				}
			}
			else if ("1 - 2 times per week".equals(text.toString())) {
				for (NullWritable nullWritable : arg1) {
					skip1++;
					sum++;
				}
			}
			else if ("3 - 4 times per week".equals(text.toString())) {
				for (NullWritable nullWritable : arg1) {
					skip3++;
					sum++;
				}
			}
			else if ("Daily or almost every day".equals(text.toString())) {
				for (NullWritable nullWritable : arg1) {
					skip7++;
					sum++;
				}
			}
		}
		@Override
		protected void cleanup(Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			Text text = new Text();
			text.set(String.format("never: %.2f%%\n1-2 times per week: %.2f%%\n3-4 times per week: %.2f%%\nalmost everyday: %.2f%%", 
					skip0/sum*100,skip1/sum*100,skip3/sum*100,skip7/sum*100));
			context.write(text, NullWritable.get());
		}
	}
}

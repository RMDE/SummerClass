package org.nh;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class UserInfo {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"UserInfo");  
		job.setJarByClass(UserInfo.class);
		
		job.setMapperClass(MyMapper.class);  
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class); 
		
		FileInputFormat.addInputPath(job, new Path("/game/resource/game.log"));
		Path outputPath = new Path("/game/UserInfo");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isSuccessful=job.waitForCompletion(true);
		System.exit(isSuccessful?0:1);
	}
	
	public static class MyMapper extends Mapper<Text, Text, Text,Text>{
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	/**
	 *reduce传入：<key,{value1,value2,....,valueN}>
	 */
	public static class MyReducer extends Reducer<Text,Text, Text,NullWritable>{
		private Text outputkey=new Text();
		private int total_user=0;
		private int total_count=0;
		private long duration =0;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			total_user++;
			for (Text value : values) {
				total_count++;
				duration+=Long.parseLong(value.toString().split("\\s+")[4]);
			}
		}
		@Override
		protected void cleanup(Context context) //当所有reduce执行完之后再执行，用于总体数据处理
				throws IOException, InterruptedException {
			outputkey.set("TotalUser:"+total_user+"\nTotalCount:"+total_count+
					"\nDuration per User:"+(duration/total_user)/3600+"\nDuration per Count:"+(duration/total_count)/3600);
			context.write(outputkey, NullWritable.get());
		}
			
	}
	
	
}

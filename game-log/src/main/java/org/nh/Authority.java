package org.nh;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

/**
 * @author 猫猫
 *Authority:每个用户登录的次数
 *根据字段，可以知晓用户是在哪一天登录
 */
public class Authority {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"Authority");  //为该job命名，名字无影响。一般用类名
		job.setJarByClass(UserInfo.class);		
		job.setMapperClass(MyMapper.class);  //此处代表设置mapper类
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/game/resource/game.log"));
		Path outputPath = new Path("/game/Authority");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isSuccessful=job.waitForCompletion(true);
		System.exit(isSuccessful?0:1);
	}
	
	public static class MyMapper extends Mapper<Text, Text, Text,IntWritable>{
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			int d=Integer.parseInt(((value.toString().split("\\s+")[2]).split("T")[0]).split("-")[2]);
			//d=Integer.parseInt(value.toString().split("\\s+")[2].substring(8, 10));   subStringj截取取前不取后 [8,10)
			IntWritable day = new IntWritable();
			day.set(d);
			context.write(key, day);
		}
	}
	
	public static class MyReducer extends Reducer<Text,IntWritable, Text,NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) 
				throws IOException, InterruptedException {
			int res=0;
			/*for (IntWritable intWritable : values) {
				res|=1<<(intWritable.get()-1);
			}*/
			Set<Integer> on = new HashSet<Integer>();
			for (IntWritable value : values) {
				on.add(value.get());
			}
			for (Integer a : on) {
				res+=Math.pow(2,a-1);
			}
			Text text= new Text();
			/*res=(1<<7)| res;*/
			text.set(key+"    \t"+res/*Integer.toBinaryString(res).substring(1)*/);
			context.write(text, NullWritable.get());
		}
	}
}

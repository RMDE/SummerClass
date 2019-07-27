package org.nh;

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

public class NewOldUser {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"NewOldUser");  //为该job命名，名字无影响。一般用类名
		job.setJarByClass(UserInfo.class);

		job.setMapperClass(MyMapper.class);  //此处代表设置mapper类
		job.setReducerClass(MyReducer.class);
		
		//指定mapper的输出类型，若mapper的输出类型和reduce的输出类型相同，则可以省略
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		//!!!!!important!!!!!
		FileInputFormat.addInputPath(job, new Path("/game/DatePartition/part-r-00000"));
		FileInputFormat.addInputPath(job, new Path("/game/DatePartition/part-r-00001"));
		Path outputPath = new Path("/game/NewOldUser");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isSuccessful=job.waitForCompletion(true);
		System.exit(isSuccessful?0:1);
	}
	
	public static class MyMapper extends Mapper<Text, Text, Text,IntWritable>{
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] data=value.toString().split("\\s+");
			String date = data[2].split("T")[0];
			String day = date.split("-")[2];
			IntWritable num = new IntWritable();
			num.set(Integer.parseInt(day));
			context.write(key, num);
		}
	}
	
	public static class MyReducer extends Reducer<Text,IntWritable, Text,NullWritable>{
		private int all_user=0;
		private int old_user =0;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> num,Context context) 
				throws IOException, InterruptedException {
			/*for (IntWritable intWritable : num) {
				if (intWritable.get()==1) {
					all_user+=1;
					old_user+=1;
					break;
				}
				else if (intWritable.get()==2) {
					all_user+=1;
					break;
				}
			}*/
			boolean fDay=false;
			boolean sDay=false;
			for (IntWritable intWritable : num) {
				if (intWritable.get()==1) {
					fDay=true;
				}
				else
					sDay=true;
			}
			if (fDay&&sDay) {
				old_user++;
				all_user++;
			}
			else {
				all_user++;
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Text text = new  Text();
			text.set("NewUser:"+(all_user-old_user)+"\nOldUser:"+old_user);
			context.write(text, NullWritable.get());
		}
	}
}

package org.nh;

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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DatePartition {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"DatePartition");  //Ϊ��job������������Ӱ�졣һ��������
		job.setJarByClass(DatePartition.class);

		job.setMapperClass(MyMapper.class);  //�˴���������mapper��
		job.setReducerClass(MyReducer.class);
		
		//partitioner
		job.setPartitionerClass(Date_Parition.class);
		job.setNumReduceTasks(7);  //������Ŀ
		
		//ָ��mapper��������ͣ���mapper��������ͺ�reduce�����������ͬ�������ʡ��
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(	NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/game/resource/game.log"));
		Path outputPath = new Path("/game/DatePartition");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isSuccessful=job.waitForCompletion(true);
		System.exit(isSuccessful?0:1);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text,IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] data=value.toString().split("\\s+");
			String date = data[3].split("T")[0];
			String day = date.split("-")[2];
			IntWritable num = new IntWritable();
			num.set(Integer.parseInt(day));
			context.write(value, num);
		}
	}
	
	public static class MyReducer extends Reducer<Text,IntWritable, Text,NullWritable>{
		@Override
		protected void reduce(Text text, Iterable<IntWritable> num,Context context)
				throws IOException, InterruptedException {
			context.write(text, NullWritable.get());
		}
	}
	
	public static class Date_Parition extends Partitioner<Text, IntWritable>{
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			int i = value.get();
			switch (i) {
			case 1:
				return  0;
			case 2:
				return 1;
			case 3: 
				return 2;
			case 4:
				return 3;
			case 5:
				return 4;
			case 6:
				return 5;
			default:
				return 6;
			}
//			for (int j = 0; j < numPartitions+1; j++) {
//				if (i==j) {
//					return j%numPartitions;
//				}
//			}
		}
	}
	
}

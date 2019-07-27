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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HoursComputer {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"HoursComputer"); 
		job.setJarByClass(WakeTime.class);
		
		job.setMapperClass(MyMapper.class);  
		job.setReducerClass(MyReducer.class);
		

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/StackOverflow/resource/生活习惯.txt"));
		FileInputFormat.addInputPath(job, new Path("/StackOverflow/resource/2017提取.txt"));
		Path outputPath = new Path("/StackOverflow/result/HoursComputer");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isSuccessful=job.waitForCompletion(true);
		System.exit(isSuccessful?0:1);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable,Text>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] words=value.toString().split("\t+");
			int flag=0;  //0表示数据为2017年
			Text text=new Text();
			text.set("0");
			if (words.length==4) {
				flag=1;
				text.set(words[2]);
			}
			else if(words.length==8){
				text.set(words[3]);
			}
			IntWritable year=new IntWritable();
			year.set(flag);
			context.write(year,text);
		}
	}
	
	public static class MyReducer extends Reducer<IntWritable,Text, Text,NullWritable>{
		private static double sum_18=0,sum_17=0;
		private static int hour1=0,hour5=0,hour9=0,hour0=0,hour12=0,count_18=0,count_17=0;
			@Override
			protected void reduce(IntWritable year, Iterable<Text> texts,Context context) 
					throws IOException, InterruptedException {
				if (year.get()==1) {//18年
					for (Text text : texts) {
						if ("Less than 1 hour".equals(text.toString())) {
							hour0++;
							sum_18+=0.5;
							count_18++;
						}
						else if ("1 - 4 hours".equals(text.toString())) {
							hour1++;
							sum_18+=2.5;
							count_18++;
						}
						else if ("5 - 8 hours".equals(text.toString())) {
							hour5++;
							sum_18+=6.5;
							count_18++;
						}
						else if ("9 - 12 hours".equals(text.toString())) {
							hour9++;
							sum_18+=10.5;
							count_18++;
						}
						else if ("Over 12 hours".equals(text.toString())) {
							hour12++;
							sum_18+=14;
							count_18++;
						}
					}
				}
				else {
					for (Text text : texts) {
						if (!"NA".equals(text.toString())&&Integer.parseInt(text.toString())>1) {
							sum_17+=Integer.parseInt(text.toString());
							count_17++;
						}						
					}
				}
			}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Text text = new Text();
			text.set(String.format("average hour of 2017: %.2f",sum_17/count_17)
						+String.format("\naverage hour of 2018: %.2f",sum_18/count_18)+
						"\nLess than 1 hour: "+hour0+"\n1 - 4 hours: "+hour1+"\n5 - 8 hours: "+hour5+
						"\n9 - 12 hours: "+hour9+"\nOver 12 hours: "+hour12);
			context.write(text, NullWritable.get());
		}
	}
}

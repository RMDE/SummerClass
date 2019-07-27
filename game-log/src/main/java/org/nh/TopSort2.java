package org.nh;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopSort2 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"TopSort");  //为该job命名，名字无影响。一般用类名
		job.setJarByClass(TopSort2.class);
		
		//sort
		job.setSortComparatorClass(MySort.class);
		job.setGroupingComparatorClass(MySort.class);
				
		job.setMapperClass(MyMapper.class);  //此处代表设置mapper类
		job.setReducerClass(MyReducer.class);
		
		//job.setMapOutputKeyClass(theClass);
		//job.getMapOutputValueClass()
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/game/TopN/part-r-00000"));
		Path outputPath = new Path("/game/TopSort");
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
			System.err.println("----");
			context.write(value,NullWritable.get() );
		}
	}
	
	public static class MyReducer extends Reducer<Text,NullWritable, Text,NullWritable>{
		@Override
		protected void reduce(Text arg, Iterable<NullWritable> arg1,Context context) 
				throws IOException, InterruptedException {
			System.err.println("====");
			String[] data = arg.toString().split("\\s+");
			Text text=new Text();
			text.set(data[0]+"    "+data[1]+"    "+data[3]);
			context.write(text, NullWritable.get());
		}
	}
	public class MySort extends WritableComparator{
		public MySort() {
			super(Text.class,true); //防止出现空指针
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text aText=(Text)a;
			Text bText=(Text)b;
			Integer atime=Integer.parseInt(aText.toString().split("\\s+")[2]);
			Integer acount=Integer.parseInt(aText.toString().split("\\s+")[1]);
			Integer aduration=Integer.parseInt(aText.toString().split("\\s+")[0]);
			Integer btime=Integer.parseInt(bText.toString().split("\\s+")[2]);
			Integer bcount=Integer.parseInt(bText.toString().split("\\s+")[1]);
			Integer bduration=Integer.parseInt(bText.toString().split("\\s+")[0]);
			if (aduration.equals(bduration)) {
				if (acount.equals(bcount)) {
					return atime.compareTo(btime);
				}
				else {
					return acount.compareTo(bcount);
				}
			}
			else {
				return aduration.compareTo(bduration);
			}
		}
		
	}
	
	
}

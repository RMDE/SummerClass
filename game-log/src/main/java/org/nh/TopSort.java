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

public class TopSort {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"TopSort");  //为该job命名，名字无影响。一般用类名
		job.setJarByClass(TopSort.class);
		
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
			context.write(value,NullWritable.get() );
		}
	}
	public static class MyReducer extends Reducer<Text,NullWritable, Text,NullWritable>{
		static private int flag=0;
		@Override
		protected void reduce(Text arg, Iterable<NullWritable> arg1,Context context) 
				throws IOException, InterruptedException {
			if (flag>=20) {
				return;
			}
			String[] data = arg.toString().split("\\s+");
			Text text=new Text();
			text.set(data[0]+"    "+data[1]+"    "+data[2]+"    "+data[4]);
			context.write(text, NullWritable.get());
			flag++;
		}
	}
	public static class MySort extends WritableComparator{
		public MySort() {
			super(Text.class,true); //防止出现空指针
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text aText=(Text)a;
			Text bText=(Text)b;
			Integer atime=Integer.parseInt(aText.toString().split("\\s+")[3]);
			Integer acount=Integer.parseInt(aText.toString().split("\\s+")[2]);
			Integer aduration=Integer.parseInt(aText.toString().split("\\s+")[1]);
			Integer btime=Integer.parseInt(bText.toString().split("\\s+")[3]);
			Integer bcount=Integer.parseInt(bText.toString().split("\\s+")[2]);
			Integer bduration=Integer.parseInt(bText.toString().split("\\s+")[1]);
//			if (aduration.equals(bduration)) {
//				if (acount.equals(bcount)) {
//					return btime.compareTo(atime);
//				}
//				else {
//					return bcount.compareTo(acount);
//				}
//			}
//			else {
//				return bduration.compareTo(aduration);
//			}
			return bduration.compareTo(aduration)==0? 
					(bcount.compareTo(acount)==0?btime.compareTo(atime):bcount.compareTo(acount))
					:bduration.compareTo(aduration);
		}
		
	}
	
	
}

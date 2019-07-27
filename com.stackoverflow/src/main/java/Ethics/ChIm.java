package Ethics;

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

public class ChIm {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"ChIm"); 
		job.setJarByClass(ChIm.class);		
		job.setMapperClass(MyMapper.class); 
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/StackOverflow/resource/道德层面.txt"));
		Path outputPath = new Path("/StackOverflow/result/ChIm");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isSuccessful=job.waitForCompletion(true);
		System.exit(isSuccessful?0:1);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text,Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Text text1=new Text();
			text1.set(value.toString().split("\t+")[2]);
			Text text2=new Text();
			text2.set(value.toString().split("\t+")[1]);
			context.write(text1, text2);
		}
	}
	
	public static class MyReducer extends Reducer<Text,Text, Text,NullWritable>{
		private static float sum12=0F,sum5=0F,sumx=0F;
		private static int count12=0,count5=0,countx=0;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if ("30 or more years".equals(key.toString())||"24-26 years".equals(key.toString())||"18-20 years".equals(key.toString())
					||"15-17 years".equals(key.toString())||"12-14 years".equals(key.toString())||"15-17 years".equals(key.toString())
					||"21-23 years".equals(key.toString())||"27-29 years".equals(key.toString())) {
				for (Text value : values) {
					if (!"NA".equals(value.toString())) {
						sum12+=1;
					}
					if ("Yes".equals(value.toString())) {
						count12++;
					}
				}
			}
			if ("3-5 years".equals(key.toString())||"0-2 years".equals(key.toString())) {
				for (Text value : values) {
					if (!"NA".equals(value.toString())) {
						sum5+=1;
					}
					if ("Yes".equals(value.toString())) {
						count5++;
					}
				}
			}
			if ("9-11 years".equals(key.toString())||"6-8 years".equals(key.toString())) {
				for (Text value : values) {
					if (!"NA".equals(value.toString())) {
						sumx+=1;
					}
					if ("Yes".equals(value.toString())) {
						countx++;
					}
				}
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Text text=new Text();
			text.set(String.format("more than 12 years: %.2f%%\n5~12 years: %.2f%%\nless than 5 years: %.2f%%", 
						count12/sum12*100,countx/sumx*100,count5/sum5*100));
			context.write(text, NullWritable.get());
		}
	}
	
	
}

package Advertisement;

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

public class Priority {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"Priority"); 
		job.setJarByClass(Priority.class);
		
		//sort
		//job.setSortComparatorClass(MySort.class);
		//job.setGroupingComparatorClass(MySort.class);		
		job.setMapperClass(MyMapper.class); 
		job.setReducerClass(MyReducer.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/StackOverflow/resource/广告观.txt"));
		Path outputPath = new Path("/StackOverflow/result/Priority");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isSuccessful=job.waitForCompletion(true);

		System.exit(isSuccessful?0:1);
	}
	
	public static class MyMapper extends Mapper<Text, Text, Text,NullWritable>{
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split("\t+");
			String str=words[3]+" "+words[4]+" "+words[5]+" "+words[6]+" "+words[7]+" "+words[8]+" "+words[9];
			Text text=new Text();
			text.set(str);
			context.write(text, NullWritable.get());
		}
	}
	
	public static class MyReducer extends Reducer<Text,NullWritable, Text,NullWritable>{
		 private static double[] score= {0,0,0,0,0,0,0};
		 private static int count=0;
		@Override
		protected void reduce(Text values, Iterable<NullWritable> arg1,Context context) 
				throws IOException, InterruptedException {
			String[] words =values.toString().split("\\s+");
			if (!"NA".equals(words[0])) {
				for (NullWritable iNullWritable : arg1) {
					for (int i = 0; i < words.length; i++) {
						score[i]+=Integer.parseInt(words[i]);
					}
					count++;
				}
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Text text=new Text();
			for (int i = 0; i < score.length; i++) {
				text.set(String.format("selection%d: %.2f", i,score[i]/count));
				context.write(text, NullWritable.get());
			}
		}
	}
	
	
}

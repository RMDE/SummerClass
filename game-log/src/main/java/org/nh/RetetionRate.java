package org.nh;

import java.io.IOException;
import java.text.NumberFormat;

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

public class RetetionRate {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"RetentionRate");  //为该job命名，名字无影响。一般用类名
		job.setJarByClass(UserInfo.class);

		job.setMapperClass(MyMapper.class);  //此处代表设置mapper类
		job.setReducerClass(MyReducer.class);
		
		//指定mapper的输出类型，若mapper的输出类型和reduce的输出类型相同，则可以省略
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/game/resource/game.log"));
		Path outputPath = new Path("/game/Retention");
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
		private int two_user=0;
		private int three_user =0;
		private int seven_user=0;
		private int all_user2=0;
		private int all_user3=0;
		private int all_user7=0;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> num,Context context) 
				throws IOException, InterruptedException {
			int N =6;
			boolean[] data=new boolean[7];
			//可以使用集合，每次使用set.add（）添加元素，然后根据集合长度判断是否七天都登陆了
			for (int i = 0; i < data.length; i++) {
				data[i]=false;
			}
			for (IntWritable intWritable : num) {
				data[intWritable.get()-1]=true;
			}
			if (data[N-1]&&data[N]) {
				two_user++;
			}
			if (data[N-2]&&data[N-1]&&data[N]) {
				three_user++;
			}
			if (data[N-6]&&data[N-5]&&data[N-4]&&data[N-3]&&data[N-2]&&data[N-1]&&data[N]) {
				seven_user++;
			}
			if (data[N-1]||data[N]) {
				all_user2++;
			}
			if (data[N-2]||data[N-1]||data[N]) {
				all_user3++;
			}
			if (data[N-6]||data[N-5]||data[N-4]||data[3]||data[N-2]||data[N-1]||data[N]) {
				all_user7++;
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Text text = new  Text();
			/*float rate_2,rate_3,rate_7;
			rate_2=(float)two_user/all_user2;
			rate_3=(float)three_user/all_user3;
			rate_7=(float)seven_user/all_user7;*/
			String rate_2=String.format("%.2f",(float)two_user/all_user2*100);
			String rate_3=String.format("%.2f",(float)three_user/all_user3*100);
			String rate_7=String.format("%.2f",(float)seven_user/all_user7*100);
			/*百分数转换
			NumberFormat nt = NumberFormat.getPercentInstance();//获取格式化对象
		    nt.setMinimumFractionDigits(2);//设置百分数精确度2即保留两位小数
		    System.out.println("百分数1：" + nt.format(rate_2));//最后格式化并输出*/
			text.set("Two days retention rate:"+rate_2+
					"%\nThree days retention rate:"+rate_3+
					"%\nSeven days retention rate:"+rate_7+"%");
			context.write(text, NullWritable.get());
		}
	}
}

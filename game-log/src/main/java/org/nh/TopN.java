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


public class TopN {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf,"TopN");  //Ϊ��job������������Ӱ�졣һ��������
		job.setJarByClass(TopN.class);
		
		job.setMapperClass(MyMapper.class);  //�˴���������mapper��
		job.setReducerClass(MyReducer.class);
		

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/game/resource/game.log"));
		Path outputPath = new Path("/game/TopN");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isSuccessful=job.waitForCompletion(true);
		System.exit(isSuccessful?0:1);
	}
	
	public static class MyMapper extends Mapper<Text, Text, Text,Text>{
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String date =value.toString().split("\\s+")[2];
			int day=Integer.parseInt((date.split("T")[0]).split("-")[2]);
			String[] words=date.split("T")[1].split(":");
			int s=0;
			for (int i =0,j=3600; i <words.length; i++,j/=60) {   //�������ڣ����Բ�����ֱ���ַ����Ƚ�
				s+=Integer.parseInt(words[i])*j;							//�����ַ�����ֻҪ����new String()���д����ģ��Ƚ�ʱ���Ǹ����ַ������д�С�Ƚϵ�
			}
			IntWritable time= new IntWritable();
			time.set(day*24*3600+s);
			String duration=value.toString().split("\\s+")[4];
			Text text=new Text();
			text.set(time+"   "+duration+"  "+date);
			context.write(key, text);
		}
	}
	
	public static class MyReducer extends Reducer<Text,Text, Text,NullWritable>{
		@Override
		protected void reduce(Text id, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			int count=0;
			int duration=0;
			int time=24*8*3600;  //�ʱ�䣬��7��ĩ
			int a;
			String date="";
			/*
			 * TreeSet::���ϣ��Զ�����
			 * Set<String> date = new TreeSet<String>();
			 */
			for (Text data : value) { 
				a=Integer.parseInt(data.toString().split("\\s+")[0]);
				if(a<time) {
					time=a;
					date=data.toString().split("\\s+")[2];
				}
				a=Integer.parseInt(data.toString().split("\\s+")[1]);
				duration+=a;
				count++;
			}
			Text text=new Text();
			text.set(id+"    "+duration+"    "+count+"    "+time+"    "+date);
			context.write(text, NullWritable.get());
		}
	}
	
}

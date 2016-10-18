package com.workec.tel.mr;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
//import java.text.SimpleDateFormat;
//import java.text.ParseException;

public class TextSingleUser extends Configured implements Tool {

	//日期格式，未使用
	//private static final String dateFormat = "MM/dd/yyyy";
	//Mapper的K2,V2默认以半角逗号分隔，在测试阶段V1也使用了此分隔符
	private static final String mapSecp = ",";
	//Reducer的V2默认以制表符分隔
	private static final String reduceOutSecp = "\t";

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		//日期格式，未使用
		//private static final SimpleDateFormat fmt = new SimpleDateFormat(dateFormat);
		private StringBuffer buffer = new StringBuffer();
		private StringBuffer kBuffer = new StringBuffer();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] data = value.toString().split(mapSecp);
			String corpId = data[0];
			String userId = data[13];
			String callNumberTo = data[5];
			String startTime = data[6].substring(11,13);
			String durTime =data[8];

			buffer.setLength(0);
			//文本格式：公司ID,用户ID,被叫号码,通话起始时间,通话持续时长
			context.write(new Text(userId), new Text(buffer
						.append(corpId)
						.append(mapSecp)
						.append(userId)
						.append(mapSecp)
						.append(callNumberTo)
						.append(mapSecp)
						.append(startTime)
						.append(mapSecp)
						.append(durTime).toString()));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text>{
		//默认保留两位小数点
		private final String decimalFormat = "#.##"; 
		private DecimalFormat df = new DecimalFormat(decimalFormat);
		private StringBuffer buffer = new StringBuffer();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int callDuration = 0;//通话总时长
			int callTimes = 0;//通话次数
			int connected = 0;//接通的通话次数
			int unconnected = 0;//未接通的通话次数
			for(Text val : values){
				callTimes++;
				String[] data = val.toString().split(mapSecp);
				int dur = Integer.parseInt(data[4]);
				if(0 != dur){
					connected++;
					callDuration += dur;
				}else{
					unconnected++;
				}
			}

			//输出格式：（键）用户ID,（值序列）通过时段,通话总时长,拨打电话次数,接通率,平均通话时长
			buffer.setLength(0);
			if(0 != connected){
				context.write(new Text(key), new Text(buffer
							.append(reduceOutSecp)
							.append(callDuration)
							.append(reduceOutSecp)
							.append(callTimes)
							.append(reduceOutSecp)
							.append(df.format((float)connected / callTimes))
							.append(reduceOutSecp)
							.append(df.format((float)callDuration / connected))
							.toString()));
			}else{
				context.write(new Text(key), new Text(buffer
							.append(reduceOutSecp)
							.append(callDuration)
							.append(reduceOutSecp)
							.append(callTimes)
							.append(reduceOutSecp)
							.append(df.format((float)connected / callTimes))
							.append(reduceOutSecp)
							.append(df.format(0.00))
							.toString()));
			}
		}
	}

	public int run(String[] args) throws Exception{
		if (args.length < 2){
			System.err.println("Error, please input the inputPath and outputPath");
			System.exit(-1);
		}

		String inputPath = args[0];
		String outputPath = args[1];

		Job job = Job.getInstance(new Configuration(), " ");

		job.setJarByClass(TextSingleUser.class);
		job.setMapperClass(MyMapper.class);
		//job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new TextSingleUser(), args);
		System.exit(res);
	}
}

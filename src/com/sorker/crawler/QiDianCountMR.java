package com.sorker.crawler;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class QiDianCountMR {
//	private static Set<String> typesSets = new HashSet<>(); 		
//	private static Map<String, Integer> typesMaps = new HashMap<>();
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {		
		private final static DoubleWritable one = new DoubleWritable(1);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] lineStrings = value.toString().split("\\|\\|"); //把每个键值段分开
			for (String string : lineStrings) {
//				System.out.println(string);
				String[] FieldsStrings2 = string.split(":");//取得key和value
				if (!FieldsStrings2[1].equals("null")) {
//					if (!FieldsStrings2[1].matches("\\d+\\.\\d+") && !FieldsStrings2[1].matches("\\d+")) {		//如果包含非数字的则跳过处理
//						System.out.println(FieldsStrings2[1] + ">>>>>>>>>>>>-------------->>>>>>>>>>----------->>>>>>>>>>>>>>>>>");
//						continue; 						
//					}
					Double values = Double.parseDouble(FieldsStrings2[1]);
					context.write(new Text(FieldsStrings2[0]), new DoubleWritable(values));
//					if (values < 10 && values > 1) {
//						context.write(new Text(FieldsStrings2[0] + "Score"), new DoubleWritable(values)); //值在1-10之间的就是评分
//					}else {
//						context.write(new Text(FieldsStrings2[0] + "Book"), new DoubleWritable(values));  //其他
//					}
//					
//					if (FieldsStrings2[0].contains("_")) {
//						context.write(new Text(FieldsStrings2[0] + "Type"), one); //计算有评分的分类数量
//					}
//				}
//					else {
//					context.write(new Text(FieldsStrings2[0] + "Frezz"), one);  //计算没有评分的分类数量
				}
//				if (FieldsStrings2[0].contains("_")) { //计算各分类数量
//					if (!typesSets.contains(FieldsStrings2[0])) {
//						typesSets.add(FieldsStrings2[0]);
//						typesMaps.put(FieldsStrings2[0], 1);
//					}else {
//						int typeNum = typesMaps.get(FieldsStrings2[0]) + 1; 
//						typesMaps.remove(FieldsStrings2[0]);
//						typesMaps.put(FieldsStrings2[0], typeNum);
//					}
//				}				
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0 ;
			for (DoubleWritable doubleWritable : values) {
				sum += doubleWritable.get(); //相加值
			}
			context.write(key, new DoubleWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
//		typesSets.add("游戏_虚拟网游");
//		typesMaps.put("游戏_虚拟网游", 1);
		for(int i =1 ; i <= 11 ; i++) {  //循环11个文件夹以减少处理资源
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "click count sort");
			job.setJarByClass(QiDianCountMR.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(job, new Path("hdfs://192.168.198.129:9000/qidain/qidian_dig" + i));
			FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.198.129:9000/qidian_dig_result_second/" + i));
//			FileInputFormat.addInputPath(job, new Path("hdfs://192.168.198.129:9000/qidian_deal_result"));
//			FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.198.129:9000/qidian_dig_result_second1/" ));
			job.waitForCompletion(true);
//			System.exit(job.waitForCompletion(true)? 0 : 1);
		}

//		if (job.waitForCompletion(true)) {
//			PrintWriter writer = new PrintWriter(new File("D:/Users/linke/Desktop/qidian/countTypes.txt"));
//			for (Map.Entry<String, Integer> entrySet : typesMaps.entrySet()) {
//				 writer.println(entrySet.getKey() + ":" + entrySet.getValue());
//			}
//			writer.close();
//			System.exit(job.waitForCompletion(true)? 0 : 1);
//		}
			
		
	}
}

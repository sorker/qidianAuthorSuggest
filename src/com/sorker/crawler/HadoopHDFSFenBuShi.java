package com.sorker.crawler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javafx.scene.shape.Line;

public class HadoopHDFSFenBuShi {
	private static final String BASEPATHINTO = "hdfs://192.168.198.129:9000/qidain/";
	private static final String BASEPATHOUT = "hdfs://192.168.198.129:9000/qidian_dig2/";
	private static Path pathInto = new  Path(BASEPATHINTO);
	private static Path pathOut = new  Path(BASEPATHOUT);
	private static Configuration conf = new Configuration();
	private static FileSystem fsOut;
	private static FileSystem fsInto;
	private static String[] strings = {"qidian_dig1", "qidian_dig2", "qidian_dig3", "qidian_dig4", "qidian_dig5", "qidian_dig6", "qidian_dig7", "qidian_dig8", "qidian_dig9", "qidian_dig10", "qidian_dig11"};
	private static int count = 0;
	
	public static void main(String[] args) throws IOException {
		Random random = new Random();		
		//输出文件系统
		fsOut = pathOut.getFileSystem(conf);
		FileStatus[] fileStatus = fsOut.listStatus(pathOut);
		//输入文件系统
		fsInto = pathInto.getFileSystem(conf);
		
		//输出文件夹下所有文件路径
		for (FileStatus fileStatus2 : fileStatus) {
			String OutTxt = fileStatus2.getPath().toString(); //txt文件地址
//			System.out.println(OutTxt);
//			String sdga = "hdfs://192.168.198.129:9000/qidian_dig/";
//			System.out.println(sdga.length() + ":" + OutTxt.length());
			Path pathOutTxt = new Path(OutTxt); 
			FSDataInputStream fStreamOut = fsOut.open(pathOutTxt);
			StringBuilder builder = new StringBuilder();
			BufferedReader reader = new BufferedReader(new InputStreamReader(fStreamOut, "UTF-8"));
			String line = null;
			while((line = reader.readLine()) != null ){
				builder.append(line);
			}
			reader.close();
			
			int num = random.nextInt(11);
			//输入文件到新文件
			//随机放入文件夹
			String IntoTxt = BASEPATHINTO + strings[num] + OutTxt.substring(39);
//			System.out.println(IntoTxt);
			Path pathIntoTxt = new Path(IntoTxt);
			FSDataOutputStream fStreamInto = fsInto.create(pathIntoTxt);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fStreamInto, "UTF-8"));
			writer.write(builder.toString());
			writer.close();
			System.out.println("已处理：" + ++count);
		}
		

	}
}

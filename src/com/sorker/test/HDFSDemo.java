package com.sorker.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSDemo {

	public static void main(String[] args) throws Exception {
		// 写出内容的操作代码
				// 需要先准备一些连接HDFS的操作支持类
	
				Configuration conf = new Configuration();
				// // 所要写出的文件路径和名称
				Path path = new Path("hdfs://192.168.198.129:9000/jjjj/test.txt");
				FileSystem fs = path.getFileSystem(conf);
				
//				 // 通过文件系统取得一个IO流
//				 FSDataOutputStream os = fs.create(path);
//				 os.writeChars("Test Hello World 测试\r\nNext Line");
//				 os.close();

				FSDataInputStream is = fs.open(path);
				String temp = is.readUTF();
				System.out.println(temp);

				is.close();

	}

}

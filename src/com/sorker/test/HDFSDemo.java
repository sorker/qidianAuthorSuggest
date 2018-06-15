package com.sorker.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSDemo {

	public static void main(String[] args) throws Exception {
		// д�����ݵĲ�������
				// ��Ҫ��׼��һЩ����HDFS�Ĳ���֧����
	
				Configuration conf = new Configuration();
				// // ��Ҫд�����ļ�·��������
				Path path = new Path("hdfs://192.168.198.129:9000/jjjj/test.txt");
				FileSystem fs = path.getFileSystem(conf);
				
//				 // ͨ���ļ�ϵͳȡ��һ��IO��
//				 FSDataOutputStream os = fs.create(path);
//				 os.writeChars("Test Hello World ����\r\nNext Line");
//				 os.close();

				FSDataInputStream is = fs.open(path);
				String temp = is.readUTF();
				System.out.println(temp);

				is.close();

	}

}

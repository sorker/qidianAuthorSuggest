package com.sorker.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.sorker.dao.DBConnAndContral;

public class DealResult {
	private static final String BASEPATH = "hdfs://192.168.198.129:9000/qidian_dig_result_second1/";
	private static Path basePath = new Path(BASEPATH);
	private static Configuration conf = new Configuration();
	private static FileSystem fsOut;
	private static Map<String, Double> dealResultMap = new HashMap<>();
	
	private static final String sqlTypesNum = "INSERT INTO typesnum (types, booknum) VALUES(?, ?)";
	private static final String sqlTagsNum = "INSERT INTO tagsnum (tags, number) VALUES(?, ?)";
	private static final String sqlTypesHaveScore = "INSERT INTO typesHaveScore (types, number) VALUES(?, ?)";
	private static final String sqlTypesNoScore = "INSERT INTO typesNoScore (types, number) VALUES(?, ?)";
	private static final String sqlTypesScore = "INSERT INTO typesScore (types, scores) VALUES(?, ?)";
	private static final String sqlTypesAllNum = "INSERT INTO typesallnum (types, allbooknum) VALUES(?, ?)";
	
	public static void main(String[] args) throws IOException, SQLException {
		DBConnAndContral dbc = new DBConnAndContral();
		dbc.getConnection();
		
		fsOut = basePath.getFileSystem(conf);
//		PrintWriter writer = new PrintWriter(new File("D:/Users/linke/Desktop/qidian/result.txt")); //
//		int i = 1;
//		for(int i =1 ; i <= 11; i++) {
//			Path forPath = new Path(BASEPATH + i);
			FileStatus[] fileStatus = fsOut.listStatus(basePath);
			
//			StringBuilder builder = new StringBuilder();
			//输出文件夹下所有文件路径
			for (FileStatus fileStatus2 : fileStatus) {
				String OutTxt = fileStatus2.getPath().toString(); //txt文件地址
	//			System.out.println(OutTxt);
	//			String sdga = "hdfs://192.168.198.129:9000/qidian_dig/";
	//			System.out.println(sdga.length() + ":" + OutTxt.length());
				Path pathOutTxt = new Path(OutTxt); 
				FSDataInputStream fStreamOut = fsOut.open(pathOutTxt);
				
				BufferedReader reader = new BufferedReader(new InputStreamReader(fStreamOut, "UTF-8"));
				String line = null;
				while((line = reader.readLine()) != null ){
//					line = line.split("\t")[0] + ":" + line.split("\t")[1] + "||";
//					System.out.println(line);
					dealResultMap.put(line.split("\t")[0], Double.parseDouble(line.split("\t")[1]));
//					builder.append(line);
				}
				reader.close();
			}
//			writer.println(builder.toString());
			
//		}
//		System.out.println("finish");
//		writer.close();
		for (Map.Entry<String, Double> entrySet : dealResultMap.entrySet()) {
			String types = entrySet.getKey();
			Double Num = entrySet.getValue();
//			System.out.println(types + ":" + Num);
			if(types.contains("Frezz")) {
				dbc.SQlInsert(sqlTypesNoScore, types.substring(0, types.indexOf("Frezz")), Num);  
			}else if (types.contains("Type")) {
				dbc.SQlInsert(sqlTypesHaveScore, types.substring(0, types.indexOf("Type")), Num);
			}else
			if (types.contains("Book") && types.contains("_")) {
				dbc.SQlInsert(sqlTypesNum, types.substring(0, types.indexOf("Book")), Num/(dealResultMap.get(types.substring(0, types.indexOf("Book")) + "Type") + dealResultMap.get(types.substring(0, types.indexOf("Book")) + "Frezz")));
				dbc.SQlInsert(sqlTypesAllNum, types.substring(0, types.indexOf("Book")), Num);
			}else if (types.contains("Book") && !types.contains("_")) {
				dbc.SQlInsert(sqlTagsNum, types.substring(0, types.indexOf("Book")), Num);
			}else {
				dbc.SQlInsert(sqlTypesScore, types, Num/dealResultMap.get(types.substring(0, types.indexOf("Score")) + "Type"));
			}
			
		}
	}
}

package com.sorker.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBConnAndContral {
	private static final String DBDRIVER = "org.gjt.mm.mysql.Driver";
	private static final String DBURL = "jdbc:mysql://localhost:3306/qidian";
	private static final String DBUSER = "root";
	private static final String DBPASSWORD = "root";
	private static  Connection conn;
	
	public DBConnAndContral() {

	}
	
	public Connection getConnection() {		
		try {
			if (conn == null || conn.isClosed()) {
				Class.forName(DBDRIVER);
				conn = DriverManager.getConnection(DBURL, DBUSER, DBPASSWORD);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}

	public void close(Connection conn) {

		try {
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
//	public static void main(String[] args) throws SQLException {
//		conn = getConnection();
//		String sqlTypesNum = "INSERT INTO typesnum (types, booknum) VALUES(?, ?)";
//		SQlInsert(sqlTypesNum, "123456", 123.41);
//		String sqlTagsNum = "INSERT INTO tagsnum (tags, number) VALUES(?, ?)";
//		String sqlTypesHaveScore = "INSERT INTO typesHaveScore (types, number) VALUES(?, ?)";
//		String sqlTypesNoScore = "INSERT INTO typesNoScore (types, number) VALUES(?, ?)";
//		String sqlTypesScore = "INSERT INTO typesScore (types, scores) VALUES(?, ?)";
//		
//	}
	
	public void SQlInsert(String sql, String types, Double Num) throws SQLException {
		PreparedStatement pst = conn.prepareStatement(sql);
		pst.setString(1, types);
		pst.setDouble(2, Num);
		pst.executeUpdate();
		pst.close();
	}
}

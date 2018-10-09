package com.xc.demo;

import java.sql.Connection; 
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ResourceBundle;

public class dbconn {
	
	private static String url;
	private static String username;
	private static String password;
	private static String driver;
	
	public static final String BUNDLE_NAME = "jdbc"; //messages.properties文件和Messages类在同一个包下,包名：com.xxx.cs.mm.service  
	  
	public static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME); 
	
    public static Connection conn =null;
	
	public dbconn(){
	
	}
	
	private static boolean preconn(){
		try {
			url = RESOURCE_BUNDLE.getString("url");
			username = RESOURCE_BUNDLE.getString("username");
			password = RESOURCE_BUNDLE.getString("password");
			driver =  RESOURCE_BUNDLE.getString("driver");
		} catch (Exception e) {
			System.out.println("获取数据库参数配异常置，请连续管理员！");
			return false;
		}
		return true;
	}
	
	public static Connection getconn() throws Exception { 
		
		boolean isConn = true;
		
		if (url==null)
			preconn();
		Class.forName(driver).newInstance(); 
		
		while(isConn){
		
			try{
				
				System.out.println(url);
				System.out.println(driver);
				System.out.println(username);
				System.out.println(password);
			    
			    conn  = DriverManager.getConnection(
									url	, username, password);
			    if(conn != null){
			    	
			    	isConn = false;
			    	
			    }
			    
			}catch(SQLException e){
				
				System.out.println("连接数据库异常，等待重新连接！");
				e.printStackTrace();
				
			};
		}

        return conn;
	}
}
	



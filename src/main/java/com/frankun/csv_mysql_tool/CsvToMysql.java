package com.frankun.csv_mysql_tool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.*;
import au.com.bytecode.opencsv.*;
import com.frankun.csv_mysql_tool.utils.*;
import org.apache.commons.lang3.StringUtils;

public class CsvToMysql {

	public CsvToMysql() throws UnsupportedEncodingException {
	}

	public static void main(String[] args) {

		PropertiesLoader propertiesLoader = new PropertiesLoader("classpath:application.properties");
		String csvFile = propertiesLoader.getProperty("csvFile");
		String url = propertiesLoader.getProperty("jdbc.url");
		String username = propertiesLoader.getProperty("jdbc.username");
		String password = propertiesLoader.getProperty("jdbc.password");
		String logTable = propertiesLoader.getProperty("jdbc.logTable");
		String infoTable = propertiesLoader.getProperty("jdbc.infoTable");

        Connection con = null;
        Statement st = null;
        ResultSet rs = null;

		int count = 0;
		int totalcount = 0;
		String values = null;
		String query = "";
        try {
		
			CSVReader reader = new CSVReader(new FileReader(csvFile));
			String[] nextLine;

			con = DriverManager.getConnection(url, username, password);
			st = con.createStatement();

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			while ((nextLine = reader.readNext()) != null) {

				int num;
				boolean valid = false; //数据库中是否不存在该邮箱
				if (StringUtils.isNotBlank(nextLine[4])){
					query = "SELECT COUNT(0) FROM " + logTable + " WHERE logname = '" + nextLine[4] + "'";
					rs = st.executeQuery(query);
					while (rs.next()){
						num = rs.getInt(1);
						if (num == 0){
							valid = true;
						}
					}
				}

				//手机号长度为11，邮箱只有一个@符号，数据库中不存在该邮箱
				if (nextLine[3].length() == 11 && haveOnlyOneAt(nextLine[4]) && valid){
					Date now = new Date();
					String createTime = sdf.format(now);
					char ch = nextLine[2].charAt(0);
					Integer gender = ch == '男' ? 1 : 0;
					count++;

					//存入用户账号密码登录表
					values = "('"
							+ nextLine[0] + "','"
							+ nextLine[4] + "','"
							+ nextLine[3] + "','"
							+ createTime + "')";

					if (count > 0) {
						query  = "INSERT INTO  `" + logTable + "` ("
								+ "`user_id`,"
								+ "`logname`,"
								+ "`password`,"
								+ "`create_time`)"
								+"VALUES " + values + ";";

						st.executeUpdate(query);
						totalcount++;
					}


					//存入用户信息表
					values = "('"
							+ nextLine[0] + "','"
							+ nextLine[1] + "','"
							+ gender + "','"
							+ createTime + "')";

					if (count > 0) {
						query  = "INSERT INTO  `" + infoTable + "` ("
								+ "`user_id`,"
								+ "`username`,"
								+ "`gender`,"
								+ "`create_time`)"
								+"VALUES " + values + ";";

						st.executeUpdate(query);
						totalcount++;
					}
				}
			}
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(CsvToMysql.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        } catch(Exception e){
			e.printStackTrace();
		} finally {
            try {
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(CsvToMysql.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
        System.out.println("存入用户" + count + "个");
		System.out.println("存入记录" + totalcount + "条");
	}

	/**
	 * 判断邮箱是否只有一个@符号
	 * @param str
	 * @return
	 */
	public static boolean haveOnlyOneAt(String str) {
		int num = 0;
		for (int i=0;i<str.length();i++) {
			if ('@' == str.charAt(i)){
				num++;
			}
		}
		if (num == 1){
			return true;
		}
		return false;
	}
}
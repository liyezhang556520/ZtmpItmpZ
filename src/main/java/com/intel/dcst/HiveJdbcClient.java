package com.intel.dcst;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {
  //private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";

  /**
 * @param args
 * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
      try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    }
    System.out.println("trying to connect hive....");
    Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "liyezhan", "");
    System.out.println("hive connected.");
    Statement stmt = con.createStatement();

    
    ResultSet res = null;
    res = stmt.executeQuery("show tables");
    res = stmt.executeQuery("show databases");
    while (res.next()) {
      System.out.println(res.getString(1));
    }
    stmt.execute("drop table if exists works");
    stmt.execute("drop table staff");
    stmt.execute("drop table proj");
        
    stmt.execute("CREATE TABLE WORKS(W_EMPNUM STRING,W_PNUM STRING,W_HOURS DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");

    stmt.execute("LOAD DATA LOCAL INPATH '/home/liyezhan/work/sotc_cloud-panthera-nist-test/sotc_cloud-hive/data/files/plusd/manualSql/WORKS.csv' OVERWRITE INTO TABLE WORKS");

    stmt.execute("CREATE TABLE STAFF(S_EMPNUM STRING,S_EMPNAME STRING,S_GRADE DOUBLE,S_CITY STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");

    stmt.execute("LOAD DATA LOCAL INPATH '/home/liyezhan/work/sotc_cloud-panthera-nist-test/sotc_cloud-hive/data/files/plusd/manualSql/STAFF.csv' OVERWRITE INTO TABLE STAFF");

    stmt.execute("CREATE TABLE PROJ(P_PNUM STRING,P_PNAME STRING,P_PTYPE STRING,P_BUDGET DOUBLE,P_CITY STRING,P_STARTDATE DATE,P_ENDDATE DATE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");

    stmt.execute("LOAD DATA LOCAL INPATH '/home/liyezhan/work/sotc_cloud-panthera-nist-test/sotc_cloud-hive/data/files/plusd/manualSql/PROJ.csv' OVERWRITE INTO TABLE PROJ");

    
    
    //res = stmt.executeQuery("select s_grade from staff where s_empnum in (select w_empnum from works)");
    res = stmt.executeQuery("select s_grade from staff");
    while (res.next()) {
      System.out.println(res.getString(1));
    }   
    
  }
}

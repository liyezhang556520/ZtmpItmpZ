package com.intel.dcst.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.intel.dcst.conf.ZizConf;

public class HiveJdbcConn extends BaseJdbcConn {
  //private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
  ZizConf conf = ZizConf.getZizConf();
  
  private String driverName = conf.getHiveDriverName();

  
  public Connection getConnection() {
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    }

    try {
      // Initiate JDBC session.
      // The <password> field value is ignored in non-secure mode.
      Connection con = DriverManager.getConnection(conf.getJdbcURL(), conf.getJdbcUser(), conf.getJdbcPasswd());
      return con;
    } catch (SQLException e) {
      throw new RuntimeException(Thread.currentThread().getName() + " failed to connect to JDBC server.", e);
    }
  }
  
//  /**
//   * Execute a sequence of SQL statements. Each statement can be either select or DML/DDL statement.
//   */
//  public static void executeStatements(Connection con, String statements, ResultSetHandler rsHandler) throws SQLException {
//    Statement stmt = con.createStatement();
//    
//    // HIVE Server does not support batch execution, so we have to execute the statements one by one.
//    try {
//      int beginIndex = 0;
//      while(beginIndex < statements.length()) {
//        if (statements.substring(beginIndex).trim().length() <= 0) {
//          break;
//        }
//        int endIndex = statements.indexOf(';', beginIndex);
//        if (endIndex < 0) {
//          throw new RuntimeException("statement endding ';' not found.");
//        }
//        String statement = statements.substring(beginIndex, endIndex).trim();
//
//        logger.info("executes statement:" + String.format("%n") +
//                    "  " + statement);
//        String statementLowerCase = statement.toLowerCase();
//        if (statementLowerCase.startsWith("select") || statementLowerCase.startsWith("with")) {
//          // TODO: log query result for verfication.
//          ResultSet res = stmt.executeQuery(statement);
//          if (rsHandler != null) {
//            rsHandler.process(res);
//          }
//          res.close();
//        } else {
//          stmt.executeUpdate(statement);
//        }
//        beginIndex = endIndex + 1;
//      }
//    } finally {
//      stmt.close();
//    }
//  }
  
  @Override
  public void connect() throws SQLException {
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

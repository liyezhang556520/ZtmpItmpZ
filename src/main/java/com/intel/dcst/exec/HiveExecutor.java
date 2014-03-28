package com.intel.dcst.exec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import com.intel.dcst.conf.ZizConf;
import com.intel.dcst.jdbc.HiveJdbcConn;

public class HiveExecutor extends BaseExecutor {
  private static final Logger logger = Logger.getLogger(HiveExecutor.class
      .getName());
  ZizConf conf = ZizConf.getZizConf();

  @Override
  public void execute() {
    HiveJdbcConn hiveJdbcConn = new HiveJdbcConn();
    try {
      Connection conn = hiveJdbcConn.getConnection();
      preTransFile(conn);
      // preProcedureFile();
      //hiveJdbcConn.connect();
    } catch (SQLException e) {
      System.out.println("error occured for HiveExecutor.");
    }
  }

  // public static void main(String[] args) throws Exception
  // {
  // logger.info("dbcmp.");
  //
  // Conf.getConf().parseCommandLine(args);
  // Connection con = null;
  //
  // try {
  // con = JdbcClient.getConnection();
  //
  // runQueries(con);
  // } finally {
  // if (con != null) {
  // try {
  // con.close();
  // } catch (SQLException e) {
  // }
  // }
  // }
  //
  // System.exit(0);
  // }
  //
  // private static void runQueries(Connection con) throws SQLException {
  // Conf conf = Conf.getConf();
  //
  // String queryDirectory;
  // String resultDirectory;
  // if (conf.isGetRefResult()) {
  // // Get the directory containing the queries
  // queryDirectory = conf.getProperty("RefQueryDirectory");
  // resultDirectory = conf.getProperty("RefResultDirectory");
  // } else {
  // queryDirectory = conf.getProperty("TestQueryDirectory");
  // resultDirectory = conf.getProperty("TestResultDirectory");
  // }
  // // Enumerate all .q files in the directory
  // File queryDir = new File(queryDirectory);
  // File resultDir = new File(resultDirectory);
  //
  //
  // // JDBC session initialization
  // File initCmdFile = new File(queryDir, "db.ini");
  // if (initCmdFile.isFile()) {
  // String initCmds = Utility.readSqlFile(initCmdFile);
  // JdbcClient.executeStatements(con, initCmds, null);
  // }
  //
  // File[] files = queryDir.listFiles();
  //
  // if (files == null) {
  // return;
  // }
  //
  // List<String> errorQueryList = new ArrayList<String>();
  // for (File file : files) {
  // if (file.isDirectory() || !file.getName().endsWith(".q")) {
  // continue;
  // }
  //
  // logger.info("query: " + file.getName());
  //
  // // If the corresponding result file exists and overwrite=false,
  // // then skip this query.
  // File resultFile = new File(resultDir, file.getName() + ".out");
  // if (!resultFile.isFile() || conf.isOverwrite()) {
  // // For each .q file, read it
  // String query = Utility.readSqlFile(file);
  // // Save the result set into .q.out file
  // SaveQueryResult sr = new SaveQueryResult(resultFile);
  // // Execute the query read from the .q file
  // JdbcClient.executeStatements(con, query, sr);
  // }
  //
  // if (!conf.isGetRefResult()) {
  // // Compare result
  // try {
  // if (SQLTestUtil.chechResultbyDB(resultFile.getName(), resultDirectory,
  // conf.getProperty("RefResultDirectory")) != 0) {
  // errorQueryList.add(file.getName());
  // }
  // } catch (Exception e) {
  // throw new RuntimeException("Exception in compare result.", e);
  // }
  // }
  // }
  //
  // if (!conf.isGetRefResult()) {
  // if (errorQueryList.size() > 0) {
  // System.out.println("!!! result is in-correct: " + errorQueryList);
  // } else {
  // System.out.println("result is correct.");
  // }
  // }
  // }

  /**
   * replace template words in q file
   */
  public void preProcedureFile() throws RuntimeException {
    String dataPath = conf.getBaseDir() + conf.getOriQueryFileDir();
    String queryFilePath = conf.getBaseDir() + conf.getQueryFileDir();
    File[] qFiles = new File(dataPath).listFiles();
    for (File qf : qFiles) {
      if (qf.isDirectory()) {
        continue;
      }

      BufferedReader br;
      try {
        br = new BufferedReader(new FileReader(qf));
        FileOutputStream nb = new FileOutputStream(new File(queryFilePath
            + qf.getName()));
        String line;
        while ((line = br.readLine()) != null) {
          if (line.startsWith("--")) {
            continue;
          }
          line = line.replaceAll("_datapath_/plusd/", dataPath);
          nb.write((line + "\n").getBytes());
        }
        nb.close();
        br.close();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  public void preTransFile(Connection conn) throws RuntimeException,
      SQLException {
    String dataPath = conf.getBaseDir() + conf.getOriQueryFileDir();
    String queryFilePath = conf.getBaseDir() + conf.getQueryFileDir();
    Statement stmt = conn.createStatement();
    ResultSet res = null;

    File[] qFiles = new File(dataPath).listFiles();
    for (File qf : qFiles) {
      if (qf.isDirectory()) {
        continue;
      }

      BufferedReader br;
      try {
        br = new BufferedReader(new FileReader(qf));
        FileOutputStream nb = new FileOutputStream(new File(queryFilePath
            + qf.getName()));
        logger.info("executing file : " + qf.getName() + "============");
        String setStr = "set hive.panthera.mode=off;";
        nb.write((setStr + "\n").getBytes());
        String query = readSqlFile(qf);
        executeStatements(conn, query, nb);
        String line;
//        while ((line = br.readLine()) != null) {
//          if (line.startsWith("--")) {
//            continue;
//          }
//          line = line.replaceAll("_datapath_/plusd/", dataPath);
//          nb.write((line + "\n").getBytes());
//        }
        nb.close();
        br.close();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  public String readSqlFile(File file) {
    String sql = "";

    try {
      BufferedReader r = new BufferedReader(new InputStreamReader(
          new FileInputStream(file), "US-ASCII"));

      while (true) {
        String line = r.readLine();
        if (line == null) {
          break;
        }

        // line = line.trim();
        if (line.isEmpty() || line.startsWith("--")) {
          continue;
        }

        sql += ("\n" + line);
      }
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Can't read file", e);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Can't read file", e);
    } catch (IOException e) {
      throw new RuntimeException("Can't read file", e);
    }

    if (sql.isEmpty()) {
      throw new RuntimeException("Empty file: " + file);
    }

    return sql;
  }

  public void executeStatements(Connection con, String statements,
      FileOutputStream nb) throws RuntimeException, SQLException {
    String dataPath = conf.getBaseDir() + conf.getOriQueryFileDir();
    Statement stmt = con.createStatement();

    // HIVE Server does not support batch execution, so we have to execute the
    // statements one by one.
    try {
      int beginIndex = 0;
      while (beginIndex < statements.length()) {
        if (statements.substring(beginIndex).trim().length() <= 0) {
          break;
        }
        int endIndex = statements.indexOf(';', beginIndex);
        if (endIndex < 0) {
          //throw new RuntimeException("statement endding ';' not found.");
          return;
        }
        if (statements.charAt(endIndex - 1) == '\\') {
          endIndex = statements.indexOf(';', endIndex + 1);
        }
        String statement = statements.substring(beginIndex, endIndex).trim();

        logger.info("executes statement:" + String.format("%n") + "  "
            + statement);
        String statementLowerCase = statement.toLowerCase();
        
        
        if (statementLowerCase.startsWith("select")
            || statementLowerCase.startsWith("with")) {
          // TODO: log query result for verfication.
          ResultSet res = null;
          try {
            res = stmt.executeQuery("explain plan for " + statement);
         // if (rsHandler != null) {
            // rsHandler.process(res);
            // }
            while (res.next()) {
              statement += res.getString(1);
            }
            int bIndex = statement.indexOf("TRANSFORMED SQL:");
            if (bIndex != -1) {
              bIndex = bIndex + "TRANSFORMED SQL:".length();
              int eIndex = statement.indexOf(';', bIndex);
              if (statement.charAt(eIndex - 1) == '\\') {
                eIndex = statement.indexOf(';', eIndex + 1);
              }
              statement = statement.substring(bIndex, eIndex).trim();
            }
            res.close();
          } catch (Exception e) {
            statement = "select error from table where result is error";
          }
          
          nb.write((statement+ ";\n").getBytes());
        } else {
          nb.write((statement+ ";\n").getBytes());
          statement = statement.replaceAll("_datapath_/plusd/", dataPath);
          logger.info("refreshed executes statement:" + String.format("%n") + "  "
              + statement);
          if (!statement.toLowerCase().startsWith("load")) {
            try {
            stmt.execute(statement);
            } catch (Exception e) {
              e.printStackTrace();
        
            }
          }
        }
        beginIndex = endIndex + 1;
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't read file", e);
    } finally {
      stmt.close();
    }
  }

}

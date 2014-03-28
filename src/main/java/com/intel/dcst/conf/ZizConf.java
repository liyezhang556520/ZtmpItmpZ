package com.intel.dcst.conf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;


public class ZizConf {
  private static final Logger logger = Logger.getLogger(ZizConf.class.getName());
  private static Properties props;
  private static ZizConf zizConf = new ZizConf();
  
  private ZizConf() {
    props = new Properties();
    try {
      props.load(ZizConf.class.getResourceAsStream("/conf.properties"));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);
    } catch (IOException e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);
    }
    jdbcURL = getProperty("jdbcURL");
//    jdbcPort = getProperty("jdbcPort");
    jdbcUser = getProperty("jsbcUser");
    jdbcPasswd = getProperty("jdbcPasswd");
//    logBase = getProperty("log.file.base");
    hiveDriverName = getProperty("HiveDriverName");
    baseDir = getProperty("BaseDir");
    oriQueryFileDir = getProperty("OriQueryFileDir");
    queryFileDir = getProperty("QueryFileDir");
    resultFileDir = getProperty("ResultFileDir");
    refResultFileDir = getProperty("RefResultFileDir");    
  }
  
  private String getProperty(String key) {
    String value = props.getProperty(key);
    if (value == null) {
      throw new RuntimeException(key + " not found in config file!");
    }
    return value;
  }
  String jdbcURL;
  String jdbcPort;
  String jdbcUser;
  String jdbcPasswd;
  String logBase;
  String hiveDriverName;
  String baseDir;
  String oriQueryFileDir;
  String queryFileDir;
  String resultFileDir;
  String refResultFileDir;
  
  public static ZizConf getZizConf() {
    return zizConf;
  }
  public String getJdbcURL() {
    return jdbcURL;
  }
  public String getHiveDriverName() {
    return hiveDriverName;
  }
  public String getBaseDir() {
    return baseDir;
  }
  public String getJdbcUser() {
    return jdbcUser;
  }
  public String getJdbcPasswd() {
    return jdbcPasswd;
  }
  public String getOriQueryFileDir() {
    return oriQueryFileDir;
  }
  public String getQueryFileDir() {
    return queryFileDir;
  }
  public String getResultFileDir() {
    return resultFileDir;
  }
  public String getRefResultFileDir() {
    return refResultFileDir;
  }
}

package com.intel.dcst;

import com.intel.dcst.conf.ZizConf;
import com.intel.dcst.exec.BaseExecutor;
import com.intel.dcst.exec.HiveExecutor;



public class ZizDriver {
  ZizConf conf;
  
  
  public void run() {
    conf = ZizConf.getZizConf();
    String jdbcURL = conf.getJdbcURL();
    BaseExecutor  hiveExec = new HiveExecutor();
    hiveExec.execute();
    
    System.out.println(jdbcURL);
  }
  
  public static void main(String [] args) {
    System.out.println("Hello Ziz.");
  }

}

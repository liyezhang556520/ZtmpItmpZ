package com.intel.dcst.exec.result;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class HiveResultChecker extends BaseResultChecker {
  /**
   * check test output by comparing with database output
   *
   * @param tname
   * @return
   * @throws Exception
   */
  public static int chechResultbyDB(String tname, String resultDir, String refDir) throws Exception {
    BufferedReader br = new BufferedReader(new FileReader(new File(resultDir, tname)));
    String hiveResult = "";
    String line;
    List<String> hiveList = new ArrayList<String>();
    List<String> dbList = new ArrayList<String>();
    List<String> hiveLList = new LinkedList<String>();
    List<String> dbLList = new LinkedList<String>();
    while ((line = br.readLine()) != null) {
      String[] hiveRow = line.split("\\|");

      String tmp = "";
      for (String r : hiveRow) {
        try {
          Float.valueOf(r);
          String[] ar = r.split("\\.");
          if (ar.length == 2 && ar[1].equals("0")) {
            r = ar[0];
          }
        } catch (Exception e) {

        }
        tmp += r;
        tmp += "|";
      }

      hiveResult += tmp;
      hiveList.add(tmp);
      hiveResult += "\r\n";
      hiveLList.add(tmp);

    }

    BufferedReader tr = new BufferedReader(new FileReader(new File(refDir, tname)));
    String dbResult = "";
    String l;
    while ((l = tr.readLine()) != null) {
      dbResult += l;
      dbList.add(l);
      dbResult += "\r\n";
      dbLList.add(l);
    }
    tr.close();
    br.close();
    if (!dbResult.equals(hiveResult) && !check4Number(hiveList, dbList)) {
      if (checkResultSet(hiveLList, dbLList)) {
        return 0;
      }
//      OutputStream nf = System.out;
//      nf.write("Expect:\r\n".getBytes());
//      nf.write(dbResult.getBytes());
//      nf.write("Fact:\r\n".getBytes());
//      nf.write(hiveResult.getBytes());

      return -1;
    }
    return 0;
  }

  private static boolean check4Number(List<String> hiveList, List<String> dbList) {

    double delt = 0.01f;
    if (hiveList.size() != dbList.size()) {
      return false;
    }
    for (int i = 0; i < hiveList.size(); i++) {
      String[] hs = hiveList.get(i).split("\\|");
      String[] ds = dbList.get(i).split("\\|");
      if (hs.length != ds.length) {
        return false;
      }

      for (int j = 0; j < hs.length; j++) {
        String hss = hs[j].trim();
        String dss = ds[j].trim();

        // adapter db's "NULL" to ""
        if (dss.toUpperCase().equals("NULL")) {
          dss = "";
        }
        if (hss.toUpperCase().equals("NULL")) {
          hss = "";
        }
        if(hss.getBytes()!=null&&hss.getBytes().length==1&&hss.getBytes()[0]=='\0'){
          hss="";
        }

        if (hss.equals(dss)) {
          continue;
        }

        try {
          Double hf = Double.valueOf(hss);
          Double df = Double.valueOf(dss);
          if (Math.abs(hf - df) < delt) {
            continue;
          } else {
            return false;
          }
        } catch (Exception e) {
          return false;
        }
      }
    }
    return true;

  }
  

  /**
   * check whether the hive and database have the same result
   * set, just diff in items order
   * 
   * @param hiveLList
   * @param dbLList
   * @return
   */
  private static boolean checkResultSet(List<String> hiveLList, List<String> dbLList) {

    double delt = 0.01f;
    //different size will lead to different result set
    if (hiveLList.size() != dbLList.size()) {
      return false;
    }
    //each time compare all the database results with one hive result
    //until there is one the same with hive result in the database result
    while (!hiveLList.isEmpty()) {
      String hOneRow = hiveLList.get(0);
      String[] hs = hOneRow.split("\\|");
      int i = 0;
      for (; i < dbLList.size(); i++) {
        String dbOneRow = dbLList.get(i);
        //first compare in string type, is failed, then there might
        //be none string type in the result item
        if (hOneRow.equals(dbOneRow)) {
          break;
        }
        String[] ds = dbOneRow.split("\\|");
        if (hs.length != ds.length) {
          return false;
        }

        int j = 0;
        for (; j < hs.length; j++) {
          String hss = hs[j].trim();
          String dss = ds[j].trim();

          // adapter db's "NULL" to ""
          if (dss.toUpperCase().equals("NULL")) {
            dss = "";
          }
          if (hss.toUpperCase().equals("NULL")) {
            hss = "";
          }
          if (hss.getBytes() != null && hss.getBytes().length == 1
              && hss.getBytes()[0] == '\0') {
            hss = "";
          }

          if (hss.equals(dss)) {
            continue;
          }
          
          //check for timestamp type: "yyyy-mm-dd hh:mi:ss pm"
          if (dss.contains("-") && dss.contains(":") && hss.contains("-")) {
            Timestamp dts = new Timestamp(System.currentTimeMillis());
            Timestamp hts = new Timestamp(System.currentTimeMillis());
            if (!hss.contains(":")) {
              hss += " 00:00:00";
            }
            try {
              dts = Timestamp.valueOf(dss);
              hts = Timestamp.valueOf(hss);
              if (dts.equals(hts)) {
                continue;
              }
            } catch (Exception e) {
              // do nothing
            }
          }
          
          //check for double type
          try {
            Double hf = Double.valueOf(hss);
            Double df = Double.valueOf(dss);
            if (Math.abs(hf - df) < delt) {
              continue;
            } else {
              break;   //get out of the splitted item, since the line is not the same
            }
          } catch (Exception e) {
            break;
          }
        }
        if (j != hs.length) {  // only when j equals to hs.length, the result items are the same
          continue;
        } else {
          break;
        }
      }
      if (i != dbLList.size()) {
        // remove the item from both lists if find a same one
        hiveLList.remove(hOneRow);
        dbLList.remove(i);
      } else {
        return false;
      }
    }
    return true;
  }

}

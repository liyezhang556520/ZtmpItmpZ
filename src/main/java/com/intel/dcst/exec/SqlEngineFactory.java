package com.intel.dcst.exec;

public class SqlEngineFactory extends ExecFactory {

  @Override
  public Executor generate(int type) {
    // TODO Auto-generated method stub
    if (type == 1) {
      return new HiveExecutor();
    }
    return null;
  }

}

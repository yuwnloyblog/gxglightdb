package com.yuwnloy.gxglightdb;

import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DbUtilFactory{
	private static Lock lock = new ReentrantLock();
	private static DbUtil instance;
	private DbUtilFactory(){
	}
	public static DbUtil getDbUtilInstance(){
		if(instance==null){
			lock.lock();
			try{
				if(instance==null){
					instance = new DbUtil();
				}
			}finally{
				lock.unlock();
			}
		}
		return instance;
	}
	public static DbUtil getDbUtilInstance(String configFileName) throws DbUtilException{
		return new DbUtil(configFileName);
	}
	public static void destroy(DbUtil dbUtil) throws DbUtilException{
		try {
			if(instance==dbUtil)
				instance = null;
			dbUtil.destroy();
		} catch (SQLException e) {
			throw new DbUtilException("Failed to destroy this DbUtil, maybe it have been released.");
		}
	}
}

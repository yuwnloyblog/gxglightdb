package com.yuwnloy.gxglightdb;

import java.sql.Date;
import java.sql.Timestamp;

public class DataHelper {
	public static String ToString(Object o) {
		String ret = null;
		if (o != null)
			ret = o.toString();
		return ret;
	}

	public static int ToInt(Object o) {
		int val = 0;
		if (o != null) {
			try {
				val = Integer.parseInt(o.toString().trim());
			} catch (NumberFormatException nfe) {
				// nfe.printStackTrace();
			}
		}
		return val;
	}

	public static Date ToSqlDate(Object o) {
		if (o != null) {
			java.util.Date d = (java.util.Date) o;
			return new Date(d.getTime());
		}
		return null;
	}

	public static Timestamp getTimestamp(Object o) {
		if (o == null)
			return null;
		java.util.Date d = (java.util.Date) o;
		Timestamp ts = new Timestamp(d.getTime());
		return ts;
	}

	public static double toDouble(Object o) {
		double d = 0;
		if (o != null) {
			try {
				d = Double.parseDouble(o.toString().trim());
			} catch (NumberFormatException nfe) {
				// nfe.printStackTrace();
			}
		}
		return d;
	}

	public static long ToLong(Object o) {
		long l = 0;
		if (o != null) {
			try {
				l = Long.parseLong(o.toString().trim());
			} catch (NumberFormatException nfe) {
				// nfe.printStackTrace();
			}
		}
		return l;
	}

	public static boolean ToBoolean(Object o) {
		boolean b = false;
		if (o != null) {
			try {
				b = Boolean.parseBoolean(o.toString().trim());
			} catch (NumberFormatException nfe) {
				
			}
		}
		return b;
	}
}

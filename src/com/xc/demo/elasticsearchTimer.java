package com.xc.demo;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;

public class elasticsearchTimer {
		
	    Timer timer;

		public elasticsearchTimer() {
			Date time = getTime();
			System.out.println("指定时间time=" + time);
			timer = new Timer();
			timer.schedule(new elasticsearchTimerTask(),time,24*60*60*1000);
		}

		public Date getTime() {
			Calendar calendar = Calendar.getInstance();
			calendar.set(Calendar.HOUR_OF_DAY,Integer.parseInt(dbconn.RESOURCE_BUNDLE.getString("HOUROFDAY")));
			calendar.set(Calendar.MINUTE,Integer.parseInt(dbconn.RESOURCE_BUNDLE.getString("MINUTE")));
			calendar.set(Calendar.SECOND,Integer.parseInt(dbconn.RESOURCE_BUNDLE.getString("SECOND")));
			Date time = calendar.getTime();
			return time;
		}

		public static void main(String[] args) {
			new elasticsearchTimer();
		}
	}

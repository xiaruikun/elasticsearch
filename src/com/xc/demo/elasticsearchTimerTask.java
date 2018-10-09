package com.xc.demo;

import java.util.TimerTask;

public class elasticsearchTimerTask  extends TimerTask {
	
	@Override
	public void run() {
		new elasticsearchMergeinto();
		System.out.println("sdfdsfsdfdsf");
	}

}

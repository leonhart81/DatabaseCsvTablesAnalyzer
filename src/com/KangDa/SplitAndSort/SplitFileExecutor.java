package com.KangDa.SplitAndSort;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class SplitFileExecutor {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Date startTime = new Date();
		
		List<Settings> settings = new ArrayList<>();
		settings = new SettingsFactory(args[0]).getSettings();
		
		Queue<Runnable> runners = new LinkedList<>();
		for(int i=0;i<settings.size();i++) {
			runners.offer(new SequentialSplittedByCharArrayManagersVer(settings.get(i)));
		}
		settings = null;
		
		while(!runners.isEmpty()) {
			runners.poll().run();
		}
		
		Date endTime = new Date();
		Long spend = (endTime.getTime() - startTime.getTime())/1000;
		Long sec = spend % 60;
		Long minute = ((spend - sec) / 60) % 60;
		Long hr = (((spend - sec) / 60) - minute) / 60;
		System.out.println("總處理時間: " + hr.toString() + " 小時  " + minute.toString() + " 分鐘 " + sec.toString() + " 秒");
	}
}

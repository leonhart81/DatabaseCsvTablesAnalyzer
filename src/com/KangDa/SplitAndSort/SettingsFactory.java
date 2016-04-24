package com.KangDa.SplitAndSort;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SettingsFactory {

	private String srcFolder;
	private String okPath;
	private String nokPath;
	private char headDelimiter;
	private char tailDelimiter;
	private int splitCnt;
	private Map<String, Integer> pairs;
	
	public SettingsFactory(String settingPath) {
		
		try(BufferedReader in = new BufferedReader(
				new InputStreamReader(new FileInputStream(settingPath), "UTF-8"))) {
			
			int eq = 0;
			int start = 0;
			int end = 0;
			int cur = 0;
			String line = null;
			String mode = null;
			List<String> targetFiles = new ArrayList<>();
			pairs = new LinkedHashMap<>();
			while((line = in.readLine()) != null) {
				
				if(line.indexOf('[') != -1 && line.indexOf(']') != -1) {
					mode = line;
				}
				else if((eq = line.indexOf('=')) != -1) {
					if(mode.equals("[Folder]")) {
						String property = line.substring(0, eq);
						String value = line.substring(eq+1);
						if(property.equals("SourcePath")) {
							srcFolder = value;
						}
						else if(property.equals("OKPath")) {
							okPath = value;
						}
						else if(property.equals("NOKPath")) {
							nokPath = value;
						}
					}
					else if(mode.equals("[Delimiters]")) {
						String property = line.substring(0, eq);
						String value = line.substring(eq+1);
						if(property.equals("headDelimiter")) {
							headDelimiter = value.charAt(0);
						}
						else if(property.equals("tailDelimiter")) {
							tailDelimiter = value.charAt(0);
						}
					}
					else if(mode.equals("[SplitNumbers]")) {
						String property = line.substring(0,eq);
						String value = line.substring(eq+1);
						if(property.equals("splitNumbers")) {
							splitCnt = Integer.parseInt(value);
						}
					}
					else if(mode.equals("[TargetTable]")) {
						String property = line.substring(0,eq);
						String value = line.substring(eq+1);
						
						if(property.equals("Start#")) {
							start = Integer.parseInt(value);
						}
						else if(property.equals("End#")) {
							end = Integer.parseInt(value);
						}
						else {
							cur++;
							if(cur >= start && cur <= end) {
								targetFiles.add(value);
							}
						}
					}
					else if(mode.equals("[TableColumnsCount]")) {
						String property = line.substring(0,eq);
						String value = line.substring(eq+1);
						
						for(int i=0;i<targetFiles.size();i++) {
							if(property.equals(targetFiles.get(i))) {
								pairs.put(targetFiles.get(i), Integer.parseInt(value));
							}
						}
					}
				}
			}
			
			pairs.forEach((K,V) -> System.out.println("載入檔案: " + K + "，欄位數: " + V));
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public List<Settings> getSettings() {
		
		List<Settings> settings = new ArrayList<>();
		pairs.forEach((K, V) -> {settings.add(new Settings(srcFolder + K, 
					okPath + K.substring(0, K.indexOf('.')), 
					nokPath + K.substring(0, K.indexOf('.')), 
					splitCnt, V, headDelimiter, tailDelimiter));});
		
		return settings;
	}
}

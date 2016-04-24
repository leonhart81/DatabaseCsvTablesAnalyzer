package com.KangDa.SplitAndSort;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class SingleFile {
	private List<Record> records;
	private String fileName;
	private long lower;
	private long upper;
	
	public SingleFile(long lower, long upper, String dirPath) {
		records = new ArrayList<>();
		this.lower = lower;
		this.upper = upper;
		
		String lowerStr = Long.toString(lower);
		String upperStr = Long.toString(upper);
		
		while(lowerStr.length() < 15){
			lowerStr = "0" + lowerStr;
	   	}
	   	
	   	while(upperStr.length() < 15) {
	   		upperStr = "0" + upperStr;
	   	}
		
		this.fileName = dirPath + "/" + lowerStr + "_" + upperStr + ".CSV";
	}
	
	public String getFileName() {
		return fileName;
	}
	
	public void pushIntoFile(Record record) {
		this.records.add(record);
	}
	
	public long getLower() {
		return lower;
	}
	
	public long getUpper() {
		return upper;
	}
	
	public void run() {
		StringBuilder fileBuilder = new StringBuilder();
		
		for(int i=0;i<records.size();i++)
			fileBuilder.append(records.get(i).toStringBuilder());
		
		try {
			Files.write(Paths.get(fileName), fileBuilder.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
		} catch (FileAlreadyExistsException e) {
	    	try {
				Files.write(Paths.get(fileName), fileBuilder.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	    } catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			fileBuilder.setLength(0);
		}
	}
} 

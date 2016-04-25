package com.Analyzer.SplitAndSort;

import java.util.List;

public class Record implements Comparable<Record>{
	private Long id;
	private List<String> list;
	private char headDelimiter;
	private char tailDelimiter;
	
	public Record(List<String> list2, char headDelimiter, char tailDelimiter) {
		this.list = list2;
		this.headDelimiter = headDelimiter;
		this.tailDelimiter = tailDelimiter;
		id = Long.parseLong(list2.get(0));
	}
	
	public StringBuilder toStringBuilder() {
		
		StringBuilder record = new StringBuilder();
		for(int i=0;i<list.size();i++) {
			if(headDelimiter != '\u0000')
				record.append(headDelimiter);
			record.append(list.get(i));
			if(tailDelimiter != '\u0000')
				record.append(tailDelimiter);
			if(i != list.size() - 1)
				record.append('\t');
		}
		record.append("\r\n");
		return record;
	}
	
	public StringBuilder toOriginalStringBuilder() {
		
		StringBuilder record = new StringBuilder();
		for(int i=0;i<list.size();i++) {
			record.append(list.get(i));
			if(i != list.size() - 1) 
				record.append('\t');
		}
		record.append("\r\n");
		return record;
	}
	
	public List<String> getList() {
		return this.list;
	}

	@Override 
	public String toString() {
		
		StringBuilder record = new StringBuilder();
		for(int i=0;i<list.size();i++) {
			record.append("\"").append(list.get(i)).append("\"");
			if(i != list.size() - 1)
				record.append('\t');
		}
		record.append("\r\n");
		return record.toString();
	}

	@Override
	public int compareTo(Record other) {
		// TODO Auto-generated method stub
		return (int)(this.id - other.id);
	}

	public Long getId() {
		// TODO Auto-generated method stub
		return id;
	}
}


package com.Analyzer.SplitAndSort;

import java.util.List;

public class RecordForSort implements Comparable<RecordForSort>{
	private Long id;
	private List<String> list;
	
	public RecordForSort(List<String> list2, char headDelimiter, char tailDelimiter) {
		this.list = list2;
		if(headDelimiter != '\u0000' && tailDelimiter != '\u0000')
			id = Long.parseLong(list2.get(0).substring(1, list2.get(0).length() - 1));
		else if(headDelimiter == '\u0000' && tailDelimiter != '\u0000')
			id = Long.parseLong(list2.get(0).substring(0, list2.get(0).length() - 1));
		else if(headDelimiter != '\u0000' && tailDelimiter == '\u0000')
			id = Long.parseLong(list2.get(0).substring(1));
		else
			id = Long.parseLong(list2.get(0));
	}
	
	public StringBuilder toStringBuilder() {
		
		StringBuilder record = new StringBuilder();
		for(int i=0;i<list.size();i++) {
			record.append(list.get(i));
			if(i != list.size() - 1)
				record.append('\t');
		}
		record.append("\r\n");
		return record;
	}

	@Override 
	public String toString() {
		
		StringBuilder record = new StringBuilder();
		for(int i=0;i<list.size();i++) {
			record.append(list.get(i));
			if(i != list.size() - 1)
				record.append('\t');
		}
		record.append("\r\n");
		return record.toString();
	}

	@Override
	public int compareTo(RecordForSort other) {
		// TODO Auto-generated method stub
		return (int)(this.id - other.id);
	}

	public Long getId() {
		// TODO Auto-generated method stub
		return id;
	}
}


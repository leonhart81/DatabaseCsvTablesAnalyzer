package com.Analyzer.SplitAndSort;

import java.util.List;

/*�@��row�����*/
public class RecordForSort implements Comparable<RecordForSort>{
	private Long id;
	private List<String> list;
	
	public RecordForSort(List<String> list2) {
		this.list = list2;
		id = Long.parseLong(list2.get(0).substring(1, list2.get(0).length() - 1));
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


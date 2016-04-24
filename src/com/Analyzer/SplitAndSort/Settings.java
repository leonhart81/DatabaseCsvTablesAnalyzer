package com.Analyzer.SplitAndSort;

public class Settings {
	private String sourcePath;
	private String okPath;
	private String nokPath;
	private long range;
	private int colCnt;
	private char headDelimiter;
	private char tailDelimiter;
	private boolean hasError;
	
	public Settings(String sourcePath, String okPath, String nokPath, long range, 
			int colCnt, char headDelimiter, char tailDelimiter) {
		this.sourcePath = sourcePath;
		this.okPath = okPath;
		this.nokPath = nokPath;
		this.range = range;
		this.colCnt = colCnt;
		this.headDelimiter = headDelimiter;
		this.tailDelimiter = tailDelimiter;
		this.hasError = false;
	}
	
	public void setError(boolean hasError) {
		this.hasError = hasError;
	}
	
	public boolean hasErrorOrNot() {
		return hasError;
	}
	
	public String getSourcePath() {
		return sourcePath;
	}
	
	public String getOkPath() {
		return okPath;
	}
	
	public String getNokPath() {
		return nokPath;
	}
	
	public long getRange() {
		return range;
	}
	
	public int getColCnt() {
		return colCnt;
	}
	
	public char getHeadDelimiter() {
		return headDelimiter;
	}
	
	public char getTailDelimiter() {
		return tailDelimiter;
	}
}

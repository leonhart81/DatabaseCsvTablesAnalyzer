package com.Analyzer.SplitAndSort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

	
public class SequentialSplittedByCharArrayManagersVer implements Runnable{
	
	private Settings settings;
	
	public SequentialSplittedByCharArrayManagersVer(Settings settings) {
		this.settings = settings;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		Date startTime = new Date();
		
		int colCnt = settings.getColCnt();
		int bufferSize = 4 * 1024 * 1024; //4M 個 字元
		
		String sourcePath = settings.getSourcePath();
		
		List<Long> extremes = new ArrayList<>();
		extremes.add(Long.MAX_VALUE);
		extremes.add(Long.MIN_VALUE);
		
		long fileSize = 0;
		
		List<Long> pos = new ArrayList<>();
		try {
			System.out.println("正在計算 "+ sourcePath +" 檔案大小");
			fileSize = getFileSizeInTermOfChars(sourcePath);
			
			System.out.println("共有 " + fileSize + " 個字元");
			if(bufferSize > fileSize)
				bufferSize = (int) fileSize;
			
			System.out.println("正在確認ID範圍");
			setIdRange(sourcePath, extremes, colCnt, fileSize, bufferSize);
			
			System.out.println("最小ID: " + extremes.get(0));
			System.out.println("最大ID: " + extremes.get(1));
			
			for(long i = (extremes.get(0)/settings.getRange())*settings.getRange(); i < extremes.get(1); i+=settings.getRange())
				pos.add(i);
			
			procFile(pos, extremes, sourcePath, settings.getOkPath(), settings.getNokPath(), colCnt, bufferSize, fileSize, settings.getRange());
			System.out.println(sourcePath + " 已經處理完成");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Date endTime = new Date();
		Long spend = (endTime.getTime() - startTime.getTime())/1000;
		Long sec = spend % 60;
		Long minute = ((spend - sec) / 60) % 60;
		Long hr = (((spend - sec) / 60) - minute) / 60;
		System.out.println("共耗時 " + hr.toString() + " 小時  " + minute.toString() + " 分鐘 " + sec.toString() + " 秒");
		System.out.println();
	}
	
	private void procFile(List<Long> pos, List<Long> extremes, String sourcePath, String okPath, String nokPath, int colCnt, int bufferSize, long fileSize, long range) throws IOException {
		try(BufferedReader in = new BufferedReader(
				new InputStreamReader(new FileInputStream(sourcePath), "UTF-8"))){
			
			char[] bf = new char[bufferSize];
			long workingCnt = bufferSize;
			int start = 0;
			int read = 1;
			String tmp = null;
			StringBuilder chunkbd = new StringBuilder();
			StringBuilder errorbd = new StringBuilder();
			List<Record> records = null;
			List<Record> errors = new ArrayList<>();
			List<String> list = new ArrayList<>();
			List<String> errorList = new ArrayList<>();
			String datum = null;
			String lastToken = null;
			String chunk = null;
			
			int endIndex = 0;
			int fileCnt = 0;
			File okFileDir = new File(okPath);
			boolean created = okFileDir.mkdirs();
			
			File nokFileDir = new File(nokPath);
			created = nokFileDir.mkdirs();
			
			System.out.println("正在確認路徑");
			boolean errorCondition = false;
			
			System.out.println("開始進行檔案拆分");
			while(in.read(bf) != -1) {
				chunkbd = chunkbd.append(bf);
				chunk = chunkbd.toString();
				
				if(read == 1) {
					start = chunk.indexOf('\n') + 1;
					read++;
				}
				else {
					start = 0;
				}
				
				records = new ArrayList<>();
				
				while(true) {
					try {
						if(tmp != null) {
							if(list.size() != colCnt - 1) {
								endIndex = chunk.indexOf('\t', start);
								lastToken = tmp + chunk.substring(start, endIndex);
								if((start = lastToken.indexOf('\n', start)) == -1) {
									datum = lastToken;
								}
								else {
									datum = lastToken.substring(++start);
								}
								start = endIndex + 1;
								list.add(datum);
								
								for(int i = list.size(); i < colCnt; i++) {
									if(i != colCnt - 1) {
										endIndex = chunk.indexOf('\t', start);
										datum = chunk.substring(start, endIndex);
										start = endIndex + 1;
										if (i == 0) {
											while(true) {
												try {
													Long.parseLong(datum);
													if(errorCondition) {
														errorCondition = false;
														errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
													}
													break;
												} catch (NumberFormatException e) {
													errorCondition = true;
													errorList = records.get(records.size() - 1).getList();
													if(datum.lastIndexOf("\r\n") != -1) {
														errorList.add(datum.substring(0, datum.lastIndexOf("\r\n")));
														datum = datum.substring(datum.lastIndexOf("\r\n") + 2);
													}
													else {
														if(!Character.isDigit(datum.charAt(0))) {
															errorList.add(datum);
															errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
															errorCondition = false;
															throw new StringIndexOutOfBoundsException();
														}
													}
												}
											}
										}
									}
									else {
										if(chunk.indexOf('\t', start) == -1) {
											endIndex = chunk.lastIndexOf("\r\n", start);
											datum = chunk.substring(start, endIndex); 
											start = endIndex + 1;
										}
										else {
											endIndex = chunk.indexOf('\t', start);
											lastToken = chunk.substring(start, endIndex);
											datum = lastToken.substring(0, lastToken.lastIndexOf("\r\n")); 
											start = start + lastToken.lastIndexOf("\r\n") + 2 ;
										}
									}
									list.add(datum);
								}
							}
							else {
								if(chunk.indexOf('\t', start) == -1) {
									lastToken = tmp + chunk.substring(start);
									endIndex = lastToken.lastIndexOf("\r\n", 0);
									datum = lastToken.substring(0, endIndex);
									start = endIndex - tmp.length() + 1;
								}
								else {
									lastToken = tmp + chunk.substring(start, chunk.indexOf('\t', start));
									datum = lastToken.substring(0, lastToken.lastIndexOf("\r\n"));
									start = start + lastToken.lastIndexOf("\r\n") - tmp.length() + 2 ;
								}
								list.add(datum);
							}
							tmp = null;
						}
						else {
							if(list.size() == 0) {
								for(int i=0; i < colCnt; i++) {
									if(i != colCnt - 1) {
										if((endIndex = chunk.indexOf('\t', start)) == -1) {
											tmp = chunk.substring(start);
											break;
										}
										datum = chunk.substring(start, endIndex);
										start = endIndex + 1;
										if(i == 0) {
											while(true) {
												try {
													Long.parseLong(datum);
													if(errorCondition) {
														errorCondition = false;
														errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
													}
													break;
												} catch (NumberFormatException e) {
													errorCondition = true;
													errorList = records.get(records.size() - 1).getList();
													if(datum.lastIndexOf("\r\n") != -1) {
														errorList.add(datum.substring(0, datum.lastIndexOf("\r\n")));
														datum = datum.substring(datum.lastIndexOf("\r\n") + 2);
													}
													else {
														if(!Character.isDigit(datum.charAt(0))) {
															errorList.add(datum);
															errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
															errorCondition = false;
															throw new StringIndexOutOfBoundsException();
														}
													}
												}
											}
										}
									}
									else {
										if((endIndex = chunk.indexOf('\t', start)) == -1) {
											if((endIndex = chunk.indexOf("\r\n", start)) == -1) {
												tmp = chunk.substring(start);
												break;
											}
											datum = chunk.substring(start, endIndex); 
											start = endIndex + 1;
											if(start == chunk.length() - 1) {
												list.add(datum);
												break;
											}
											else {
												list.add(datum);
												tmp = chunk.substring(++start);
												break;
											}
										}
										else {
											lastToken = chunk.substring(start, endIndex);
	    									int tmpEndIndex = 0;
	    									if((tmpEndIndex = lastToken.lastIndexOf("\r\n")) != -1) {
	    										datum = lastToken.substring(0, tmpEndIndex); 
	    										start = start + tmpEndIndex + 2 ;
	    									}
											else {
	    										errorCondition = true;
	    										errorList = list;
	    										list = new ArrayList<>();
	    										do {
	    											errorList.add(lastToken);
	    											if((endIndex = chunk.indexOf('\t', ++start)) != -1) {
	    												lastToken = chunk.substring(start, endIndex);
	    												if((tmpEndIndex = lastToken.lastIndexOf("\r\n")) != -1) {
	    													datum = lastToken.substring(0, tmpEndIndex);
	    													errorList.add(datum);
	    													start = start + tmpEndIndex + 2;
	    													errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
	    													errorList = null;
		    												break;
	    												}
	    											}
	    										}while(true);
	    									}
										}
									}
									if(!errorCondition) {
	    								list.add(datum);
	    							}
	    							else {
	    								break;
	    							}
								}
							}
							else {
								for(int i = list.size(); i < colCnt; i++) {
									if(i != colCnt - 1) {
										if((endIndex = chunk.indexOf('\t', start)) == -1) {
											tmp = chunk.substring(start);
											break;
										}
										if(endIndex == 0 && start == endIndex) {
											endIndex = chunk.indexOf('\t', ++start);
										}
										datum = chunk.substring(start, endIndex);
										start = endIndex + 1;
										if(i==0) {
											while(true) {
												try {
													Long.parseLong(datum);
													if(errorCondition) {
														errorCondition = false;
														errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
													}
													break;
												} catch (NumberFormatException e) {
													errorCondition = true;
													errorList = records.get(records.size() - 1).getList();
													if(datum.lastIndexOf("\r\n") != -1) {
														errorList.add(datum.substring(0, datum.lastIndexOf("\r\n")));
														datum = datum.substring(datum.lastIndexOf("\r\n") + 2);
													}
													else {
														if(!Character.isDigit(datum.charAt(0))) {
															errorList.add(datum);
															errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
															errorCondition = false;
															throw new StringIndexOutOfBoundsException();
														}
													}
												}
											}
										}
									}
									else {
										if(chunk.indexOf('\t', start) == -1) {
											if((endIndex = chunk.indexOf("\r\n", start)) == -1) {
												tmp = chunk.substring(start);
												break;
											}
											datum = chunk.substring(start, endIndex); 
											start = endIndex + 1;
											if(start == chunk.length() - 1) {
												list.add(datum);
												break;
											}
										}
										else {
											endIndex = chunk.indexOf('\t', start);
											lastToken = chunk.substring(start, endIndex);
											datum = lastToken.substring(0, lastToken.lastIndexOf("\r\n")); 
											start = start + lastToken.lastIndexOf("\r\n") + 2 ;
										}
									}
									list.add(datum);
								}
							}
						}
						if(list.size() == colCnt) {
							try {
								records.add(new Record(list, settings.getHeadDelimiter(), settings.getTailDelimiter()));
							} catch (NumberFormatException e) {
								
							}
							list = new ArrayList<>();
						}
						else {
	    					if(tmp != null) {
	    						break;
	    					}
	    					else { 
	    						errorCondition = false;
	    						continue;
	    					}
	    				}
					} catch (StringIndexOutOfBoundsException e) {
							//e.printStackTrace();
							System.out.println("抓到錯誤");
					} 
					if(chunk.indexOf('\t', start) == -1)
						break;
				}
				
				List<SingleFile> splittedFiles = new ArrayList<>();
				
				if(created) {
					for(int i=0;i<pos.size();i++)
						splittedFiles.add(new SingleFile(pos.get(i)+1, pos.get(i)+range, okPath));
				
					for(int i=0;i<records.size();i++) {
						long recordId = records.get(i).getId();
						int belongsTo = (int)((recordId-((extremes.get(0)/range))*range) / range);
						
						if(recordId % range != 0) {
							splittedFiles.get(belongsTo).pushIntoFile(records.get(i));
						}
						else {
							splittedFiles.get(belongsTo - 1).pushIntoFile(records.get(i));
						}
					}
				
					for(int i=0;i<splittedFiles.size();i++) {
						splittedFiles.get(i).run();
					}
				}
				
				chunkbd.setLength(0);
				
				if(fileSize - workingCnt > bufferSize) {
					workingCnt += bufferSize;
				}
				else {
					fileCnt = splittedFiles.size();
					if(bufferSize < fileSize) {
						bf = new char[(int)(fileSize - workingCnt)];
					}
				}
				
				if(workingCnt % (40 * 1024 * 1024) == 0) {
					System.out.println("已處理 " + workingCnt + " 個字元 ，尚餘 " + (fileSize - workingCnt) + " 個字元");
				}
			}
			
			System.out.println("正在產生錯誤檔案");
			String errorLog = nokPath + "\\" + nokPath.substring(nokPath.lastIndexOf('\\')) + "_error.txt"; 
			try (BufferedWriter out = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(errorLog), "UTF-8"))) {
				
				for(int i=0;i<errors.size();i++)
					errorbd.append(errors.get(i).toOriginalStringBuilder());
				
				out.write(errorbd.toString());
			}
			
			System.out.println("開始針對每個檔案進行排序");
			StringBuilder fBuilder = new StringBuilder();
			for(int i = 0; i < fileCnt; i++) {
				long lower = ((extremes.get(0)/range)*range) + i * range + 1;
				long upper = ((extremes.get(0)/range)*range) + (i+1) * range;
				String lowerStr = Long.toString(lower);
				String upperStr = Long.toString(upper);
				
				while(lowerStr.length() < 15){
					lowerStr = "0" + lowerStr;
			   	}
			   	
			   	while(upperStr.length() < 15) {
			   		upperStr = "0" + upperStr;
			   	}
			   	
			   	String filePath = okPath + "/" + lowerStr + "_" + upperStr + ".CSV";
			   	int fSize = 0;
			   	
			   	try (BufferedReader reader = new BufferedReader(
			   			new InputStreamReader(new FileInputStream(filePath), "UTF-8"))) {
			   		
			   		while(reader.read() != -1) {
			   			fSize++;
			   		}
			   			
			   	}
			   	
				try (BufferedReader reader = new BufferedReader(
			   			new InputStreamReader(new FileInputStream(filePath), "UTF-8"))) {
			   	
			   		char[] fBuffer = new char[fSize];
			   		
			   		reader.read(fBuffer);
			   		fBuilder.append(fBuffer);
	
			   		
			   		List<RecordForSort> recordsForSort = new ArrayList<>();
			   		String fileContent = fBuilder.toString();
			   		
			   		start = 0;
			   		while(true) {
				    	datum = null;
				    	lastToken = null;
						endIndex = 0;
						try {
				    		list = new ArrayList<>();
				    		for(int i1=0; i1 < colCnt; i1++) {
								if(i1 != colCnt - 1) {
									endIndex = fileContent.indexOf('\t', start);
									datum = fileContent.substring(start, endIndex);
									start = endIndex + 1;
								}
								else {
									if((endIndex = fileContent.indexOf('\t', start)) == -1) {
										endIndex = fileContent.indexOf("\r\n", start);
										datum = fileContent.substring(start, endIndex); 
										start = endIndex + 1;
									}
									else {
										lastToken = fileContent.substring(start, endIndex);
										datum = lastToken.substring(0, lastToken.lastIndexOf("\r\n")); 
										start = start + lastToken.lastIndexOf("\r\n") + 2 ;
									}
								}
								list.add(datum);
							}
				    		try {
				    			if(list.size() == colCnt)
				    				recordsForSort.add(new RecordForSort(list));
				    		} catch(NumberFormatException e){
				    			
				    		}
				    	} catch(StringIndexOutOfBoundsException e) {
				    		
				    	}
				    	if(fileContent.indexOf('\t', start) == -1)
				    		break;
				    }
				
			   		Collections.sort(recordsForSort);
			   		
			   		fBuilder.setLength(0);
			   		
			   		for(int i2=0;i2<recordsForSort.size();i2++)
			   			fBuilder.append(recordsForSort.get(i2));
			   		
			   		try(BufferedWriter out = new BufferedWriter(
			   				new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8"))){
			   			out.write(fBuilder.toString());
			   		}
			   		
			   		fBuilder.setLength(0);
			   		System.out.println(filePath + " 已經排序完成，尚餘" + (fileCnt - (i+1)) + "個檔案");
				}
			}
		}
	}
	
	private long getFileSizeInTermOfChars(String sourcePath) throws IOException {
		long fileSize = 0;
		try(BufferedReader s = new BufferedReader(
				new InputStreamReader(new FileInputStream(sourcePath), "UTF-8"))) {
			
			while(s.read() != -1) {
				fileSize++;
			}
		}
		
		return fileSize;
	}
	
	private void setIdRange(String sourcePath, List<Long> extremes,int colCnt, long fileSize, int bufferSize) throws IOException {
		int read = 1;
		int start = 0;
		int endIndex = 0;
		long currentId = 0;
		long workingCnt = bufferSize;
		boolean errorCondition = false;
		String tmp = null;
		String lastToken = null;
		String datum = null;
	    StringBuilder chunkbd = new StringBuilder();
	    String chunk = null;
	    List<String> list = new ArrayList<>();
	    List<String> errorList = new ArrayList<>();
	    List<Record> errors = new ArrayList<>();
	    
	    char[] bf = new char[bufferSize];
	    
	    try(BufferedReader in = new BufferedReader(
				new InputStreamReader(new FileInputStream(sourcePath), "UTF-8"))){
	    	while(in.read(bf) != -1) {
	    		chunkbd = chunkbd.append(bf);
	    		chunk = chunkbd.toString();
			
	    		if(read == 1) {
	    			start = chunk.indexOf('\n') + 1;
	    			read++;
	    		}
	    		else {
	    			start = 0;
	    		}
			
			
	    		while(true) {
	    			try {
	    				if(tmp != null) {
	    					if(list.size() != colCnt - 1) {
	    						endIndex = chunk.indexOf('\t', start);
	    						lastToken = tmp + chunk.substring(start, endIndex);
	    						if((start = lastToken.indexOf('\n', start)) == -1) {
	    							datum = lastToken;
	    						}
	    						else {
	    							datum = lastToken.substring(++start);
	    						}
	    						start = endIndex + 1;
	    						list.add(datum);
							
	    						for(int i = list.size(); i < colCnt; i++) {
	    							if(i != colCnt - 1) {
	    								endIndex = chunk.indexOf('\t', start);
	    								datum = chunk.substring(start, endIndex);
	    								start = endIndex + 1;
	    								if (i == 0) {
	    									while(true) {
												try {
													Long.parseLong(datum);
													if(errorCondition) {
														errorCondition = false;
														errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
													}
													break;
												} catch (NumberFormatException e) {
													errorCondition = true;
													errorList.add("123");
													if(datum.lastIndexOf("\r\n") != -1) {
														errorList.add(datum);
														datum = datum.substring(datum.lastIndexOf("\r\n") + 2);
													}
													else {
														if(!Character.isDigit(datum.charAt(0))) {
															errorList.add(datum);
															errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
															errorCondition = false;
															throw new StringIndexOutOfBoundsException();
														}
													}
												}
											}
	    								}
	    							}
	    							else {
	    								if(chunk.indexOf('\t', start) == -1) {
	    									endIndex = chunk.lastIndexOf("\r\n", start);
	    									datum = chunk.substring(start, endIndex); 
	    									start = endIndex + 1;
	    								}
	    								else {
	    									endIndex = chunk.indexOf('\t', start);
	    									lastToken = chunk.substring(start, endIndex);
	    									datum = lastToken.substring(0, lastToken.lastIndexOf("\r\n")); 
	    									start = start + lastToken.lastIndexOf("\r\n") + 2 ;
	    								}
	    							}
	    							list.add(datum);
	    						}
	    					}
	    					else {
	    						if(chunk.indexOf('\t', start) == -1) {
	    							lastToken = tmp + chunk.substring(start);
	    							endIndex = lastToken.lastIndexOf("\r\n", 0);
	    							datum = lastToken.substring(0, endIndex);
	    							start = endIndex - tmp.length() + 1;
	    						}
	    						else {
	    							lastToken = tmp + chunk.substring(start, chunk.indexOf('\t', start));
	    							datum = lastToken.substring(0, lastToken.lastIndexOf("\r\n"));
	    							start = start + lastToken.lastIndexOf("\r\n") - tmp.length() + 2 ;
	    						}
	    						list.add(datum);
	    					}
	    					tmp = null;
	    				}
	    				else {
	    					if(list.size() == 0) {
	    						for(int i=0; i < colCnt; i++) {
	    							if(i != colCnt - 1) {
	    								if((endIndex = chunk.indexOf('\t', start)) == -1) {
	    									tmp = chunk.substring(start);
	    									break;
	    								}
	    								datum = chunk.substring(start, endIndex);
	    								start = endIndex + 1;
	    								if(i == 0) {
	    									while(true) {
												try {
													Long.parseLong(datum);
													if(errorCondition) {
														errorCondition = false;
														errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
													}
													break;
												} catch (NumberFormatException e) {
													errorCondition = true;
													errorList.add("123");
													if(datum.lastIndexOf("\r\n") != -1) {
														errorList.add(datum);
														datum = datum.substring(datum.lastIndexOf("\r\n") + 2);
													}
													else {
														if(!Character.isDigit(datum.charAt(0))) {
															errorList.add(datum);
															errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
															errorCondition = false;
															throw new StringIndexOutOfBoundsException();
														}
													}
												}
											}
	    								}
	    							}
	    							else {
	    								if((endIndex = chunk.indexOf('\t', start)) == -1) {
	    									if((endIndex = chunk.indexOf("\r\n", start)) == -1) {
	    										tmp = chunk.substring(start);
	    										break;
	    									}
	    									datum = chunk.substring(start, endIndex); 
	    									start = endIndex + 1;
	    									if(start == chunk.length() - 1) {
	    										list.add(datum);
	    										break;
	    									}
	    									else {
	    										list.add(datum);
	    										tmp = chunk.substring(++start);
	    										break;
	    									}
	    								}
	    								else {
	    									lastToken = chunk.substring(start, endIndex);
	    									int tmpEndIndex = 0;
	    									if((tmpEndIndex = lastToken.lastIndexOf("\r\n")) != -1) {
	    										datum = lastToken.substring(0, tmpEndIndex); 
	    										start = start + tmpEndIndex + 2 ;
	    									}
	    									else {
	    										errorCondition = true;
	    										errorList = list;
	    										list = new ArrayList<>();
	    										do {
	    											errorList.add(lastToken);
	    											if((endIndex = chunk.indexOf('\t', ++start)) != -1) {
	    												lastToken = chunk.substring(start, endIndex);
	    												if((tmpEndIndex = lastToken.lastIndexOf("\r\n")) != -1) {
	    													datum = lastToken.substring(0, tmpEndIndex);
	    													errorList.add(datum);
	    													start = start + tmpEndIndex + 2;
	    													errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
		    												break;
	    												}
	    											}
	    										}while(true);
	    									}
	    								}
	    							}
	    							if(!errorCondition) {
	    								list.add(datum);
	    							}
	    							else {
	    								break;
	    							}
	    						}
	    					}
	    					else {
	    						for(int i = list.size(); i < colCnt; i++) {
	    							if(i != colCnt - 1) {
	    								if((endIndex = chunk.indexOf('\t', start)) == -1) {
	    									tmp = chunk.substring(start);
	    									break;
	    								}
	    								if(endIndex == 0 && start == endIndex) {
	    									endIndex = chunk.indexOf('\t', ++start);
	    								}
	    								datum = chunk.substring(start, endIndex);
	    								start = endIndex + 1;
	    								if(i==0) {
	    									while(true) {
												try {
													Long.parseLong(datum);
													if(errorCondition) {
														errorCondition = false;
														errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
													}
													break;
												} catch (NumberFormatException e) {
													errorCondition = true;
													errorList.add("123");
													if(datum.lastIndexOf("\r\n") != -1) {
														errorList.add(datum);
														datum = datum.substring(datum.lastIndexOf("\r\n") + 2);
													}
													else {
														if(!Character.isDigit(datum.charAt(0))) {
															errorList.add(datum);
															errors.add(new Record(errorList, settings.getHeadDelimiter(), settings.getTailDelimiter()));
															errorCondition = false;
															throw new StringIndexOutOfBoundsException();
														}
													}
												}
											}
	    								}
	    							}
	    							else {
	    								if(chunk.indexOf('\t', start) == -1) {
	    									if((endIndex = chunk.indexOf("\r\n", start)) == -1) {
	    										tmp = chunk.substring(start);
	    										break;
	    									}
	    									datum = chunk.substring(start, endIndex); 
	    									start = endIndex + 1;
	    									if(start == chunk.length() - 1) {
	    										list.add(datum);
	    										break;
	    									}
	    								}
	    								else {
	    									endIndex = chunk.indexOf('\t', start);
	    									lastToken = chunk.substring(start, endIndex);
	    									datum = lastToken.substring(0, lastToken.lastIndexOf("\r\n")); 
	    									start = start + lastToken.lastIndexOf("\r\n") + 2 ;
	    								}
	    							}
	    							list.add(datum);
	    						}
	    					}
	    				}
	    				if(list.size() == colCnt) {
	    					errorCondition = false;
	    					Record record = new Record(list, settings.getHeadDelimiter(), settings.getTailDelimiter());
	    					currentId = record.getId();
	    					if(currentId < extremes.get(0))
	    						extremes.set(0, currentId);
	    					if(currentId > extremes.get(1))
		    					extremes.set(1, currentId);
	    					list = new ArrayList<>();
	    				}
	    				else {
	    					if(tmp != null) {
	    						break;
	    					}
	    					else { 
	    						errorCondition = false;
	    						continue;
	    					}
	    				}
	    			} catch (StringIndexOutOfBoundsException e) {
	    				//e.printStackTrace();
	    			} 
	    			if(chunk.indexOf('\t', start) == -1)
	    				break;
	    		}
			
	    		chunkbd.setLength(0);
	    		if(fileSize - workingCnt > bufferSize) {
	    			workingCnt += bufferSize;
	    		}
	    		else {
	    			if(bufferSize < fileSize)
	    				bf = new char[(int)(fileSize - workingCnt)];
	    		}
	    		System.out.println("已處理 " + workingCnt + " 個字元 ，尚餘 " + (fileSize - workingCnt) + " 個字元");
	    	}
	    	if(errors.size() > 0) {
	    		settings.setError(true);
	    	}
	    }
	}
}
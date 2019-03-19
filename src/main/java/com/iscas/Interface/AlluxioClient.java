package com.iscas.Interface;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import com.iscas.Configuration.Configuration;

public class AlluxioClient {
	//获取文件系统FileSystem
	private static FileSystem fs = FileSystem.Factory.get();
	/**
	 * 写文件到Alluxio
	 * @param filepath 文件路径
	 * @param content 写入内容
	 */
	public boolean writeToAlluxio(String filepath,List<String> content) {
		String fullPath = Configuration.AlluxioURL+filepath;
		AlluxioURI path = new AlluxioURI(fullPath);
		BufferedWriter writer = null;
		try {
			if(!fs.exists(path)) {
                writer = new BufferedWriter(new OutputStreamWriter(fs.createFile(path)));
				for(String line:content) {
					writer.write(line);
					writer.newLine();
				}
			}
			return fs.exists(path);
		}catch(IOException e) {
			e.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(writer != null) {
					writer.close();
				}
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
	/**
	 * 从HDFS/Alluxio上读取文件
	 * @param filePath 文件路径
	 * */
	public static List<String> readFromAlluxio(String filePath) {
		List<String> list = new ArrayList<String>();
		String Fullpath = Configuration.AlluxioURL + filePath;
		AlluxioURI path = new AlluxioURI(Fullpath);
		BufferedReader reader = null;
		try {
			if(fs.exists(path)) {
				reader = new BufferedReader(new InputStreamReader(fs.openFile(path)));
				for(String line = null;(line = reader.readLine()) != null;) {
					list.add(line);
				}
				return list;
			}
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(reader != null) {
					reader.close();
				}
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
}

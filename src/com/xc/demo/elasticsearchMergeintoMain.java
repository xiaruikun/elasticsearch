package com.xc.demo;

import java.io.*;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import javax.management.RuntimeErrorException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

@SuppressWarnings("deprecation")
public class elasticsearchMergeintoMain {


	public static void main(String[] args){
		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		String path = dbconn.RESOURCE_BUNDLE.getString("zipPath") + date + ".zip";
		mergeito("all");
	}

	public static void mergeito(String judge) {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", dbconn.RESOURCE_BUNDLE.getString("clustername")).put("client.transport.ping_timeout", "120s").build();
		Client client = new TransportClient(settings).addTransportAddress(
				new InetSocketTransportAddress(dbconn.RESOURCE_BUNDLE.getString("TransportAddress2"),
						Integer.parseInt(dbconn.RESOURCE_BUNDLE.getString("TransportAddressPort"))));
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		BulkResponse bulkResponse = null;
		bulkRequest.setTimeout("5000");
		bulkRequest.setRefresh(true);
		if(judge.equals("all"))
		{
			ArrayList<String> files = getFiles();
			int count = 0;
			try {
				for (int i = 0; i < files.size(); i++) {
					String encoding = "UTF-8";
					List list = readZipFile(files.get(i));
					for(int j=0;j<list.size();j++)
					{
						count++;
						bulkRequest.add(client.prepareIndex(dbconn.RESOURCE_BUNDLE.getString("index2"), dbconn.RESOURCE_BUNDLE.getString("type2")).setSource((Map)list.get(j)));
						if (count % 1000 == 0) {
							bulkResponse = bulkRequest.execute().actionGet();
							bulkRequest = client.prepareBulk();
							if (bulkResponse.hasFailures()) {
								System.out.println("插入DOC数据出现异常" + bulkResponse.buildFailureMessage() + "<------>" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
							}
						}
					}
					if (count % 1000 != 0) {
						bulkResponse = bulkRequest.execute().actionGet();
						bulkRequest = client.prepareBulk();
					}
//				File file = new File(files.get(i));
//				if (file.isFile() && file.exists()) { //判断文件是否存在
//					InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);//考虑到编码格式
//					BufferedReader bufferedReader = new BufferedReader(read);
//					String lineTxt = null;
//					while ((lineTxt = bufferedReader.readLine()) != null) {
//						String[] line = lineTxt.split("\t");
//						Map<String, Object> ret = new HashMap<String, Object>();
//						ret.put("TYCODE", "NEWS");
//						ret.put("BOOST", "8");
//						ret.put("information_classification", line[0].toString());
//						ret.put("title", line[1].toString());
//						ret.put("abstract", line[2].toString());
//						ret.put("pubulish_date", line[3].toString());
//						ret.put("create_date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
//						ret.put("url", line[4].toString());
//						ret.put("keyword", line[5].toString());
//						ret.put("news_snapshot", line[6].toString());
//						ret.put("label", line[7].toString());
//						ret.put("news_channel", line[8].toString());
//						ret.put("datasource", line[9].toString());
//
//						bulkRequest.add(client.prepareIndex(dbconn.RESOURCE_BUNDLE.getString("index2"), dbconn.RESOURCE_BUNDLE.getString("type2")).setSource(ret));
//						if (count % 1000 == 0) {
//							bulkResponse = bulkRequest.execute().actionGet();
//							bulkRequest = client.prepareBulk();
//							if (bulkResponse.hasFailures()) {
//								System.out.println("插入DOC数据出现异常" + bulkResponse.buildFailureMessage() + "<------>" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
//							}
//						}
//					}
//					if (count % 1000 != 0) {
//						bulkResponse = bulkRequest.execute().actionGet();
//						bulkRequest = client.prepareBulk();
//					}
//					read.close();
//				} else {
//					System.out.println("找不到指定的文件");
//				}
				}

			} catch (Exception e) {
				System.out.println("读取文件内容出错");
				e.printStackTrace();
			}
			finally {

				try {
					client.close();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}
		else
		{

		}
	}

	public static ArrayList<String> getFiles() {
		ArrayList<String> files = new ArrayList<String>();
		File file = new File(dbconn.RESOURCE_BUNDLE.getString("zipPath"));
		File[] tempList = file.listFiles();

		for (int i = 0; i < tempList.length; i++) {
			if (tempList[i].isFile()) {
//              System.out.println("文     件：" + tempList[i]);
				files.add(tempList[i].toString());
			}
			if (tempList[i].isDirectory()) {
//              System.out.println("文件夹：" + tempList[i]);
			}
		}
		return files;
	}

	public static ArrayList<Map> readZipFile(String file){
		ArrayList<Map> list = new ArrayList<Map>();
		try
		{
			ZipFile zf = new ZipFile(file);
			InputStream in = new BufferedInputStream(new FileInputStream(file));
			ZipInputStream zin = new ZipInputStream(in);
			ZipEntry ze;
			int i = 1;
			while ((ze = zin.getNextEntry()) != null) {
				if (ze.isDirectory()) {

				} else {
					if (ze.getName().substring(ze.getName().lastIndexOf(".") + 1).matches("^[txt|texT|TXT|TEXT]+$")&&ze.getSize() > 0) {
						BufferedReader br = new BufferedReader(new InputStreamReader(zf.getInputStream(ze),"UTF-8"));
						String lineTxt="";
						while ((lineTxt = br.readLine()) != null) {
							if(i>=2)
							{
								Map<String, Object> ret = new HashMap<String, Object>();
								String[] line = lineTxt.split("\t");
								ret.put("TYCODE", "NEWS");
								ret.put("BOOST", "8");
								ret.put("information_classification", line[0].toString());
								ret.put("title", line[1].toString());
								ret.put("abstract", line[2].toString());
								ret.put("pubulish_date", line[3].toString());
								ret.put("create_date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
								ret.put("url", line[4].toString());
								ret.put("keyword", line[5].toString());
								ret.put("news_snapshot", line[6].toString());
								ret.put("label", line[7].toString());
								ret.put("news_channel", line[8].toString());
								ret.put("datasource", line[9].toString());
								list.add(ret);
							}
							i++;
						}
						br.close();
					}
				}
			}
			zin.closeEntry();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return list;
	}
}

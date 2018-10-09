package com.xc.demo;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
public class elasticsearchMergeinto {
	
	public elasticsearchMergeinto() {
		
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", dbconn.RESOURCE_BUNDLE.getString("clustername")).put("client.transport.ping_timeout", "120s").build();
		
		Client client = new TransportClient(settings).addTransportAddress(
				new InetSocketTransportAddress(dbconn.RESOURCE_BUNDLE.getString("TransportAddress"),
						Integer.parseInt(dbconn.RESOURCE_BUNDLE.getString("TransportAddressPort"))));
				
		try {
			dbconn.getconn();
			dbconn.conn.setAutoCommit(false);
		} catch (Exception e1) {
			throw new RuntimeErrorException(null, "数据库连接异常联系管理员！");
		}
		
		Statement statement = null;
		CallableStatement generationIncremental = null;
		ResultSet resultSetArea = null;
		
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		BulkResponse bulkResponse = null;
		bulkRequest.setTimeout("5000");

		try {
			
			statement = dbconn.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

			System.out.println("生成需要更新的开始时间："+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
			generationIncremental =dbconn.conn.prepareCall("{call P_ORACLE_TO_INCREMENT()}");
			generationIncremental.execute();
			System.out.println("生成需要更新的完成时间："+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

			System.out.println("开始mergeinto引擎数据！"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
			String SqlAddress = "select * from " + dbconn.RESOURCE_BUNDLE.getString("incrementtable");
			System.out.println("结果集查询语句：" + SqlAddress);
			resultSetArea = statement.executeQuery(SqlAddress);
			int count = 0;
			
			while (resultSetArea.next()) {
								
				String ID = resultSetArea.getString("ID");
				Map<String, Object> ifUpdated = new HashMap<String, Object>();
				/*结果集中存在ADDRESS_ID重复的数据,根据ADDRESS_ID只删除一次*/
				if(ifUpdated.containsKey(ID)){					
			
				}else{
					
					Boolean ifsucess =  true;			    	
			    	while(ifsucess){			    		
				    	try{							
							QueryBuilder query = QueryBuilders.termQuery("ID", ID);
							DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(client);
							deleteByQueryRequestBuilder.setIndices(dbconn.RESOURCE_BUNDLE.getString("index"));
							deleteByQueryRequestBuilder.setTypes(dbconn.RESOURCE_BUNDLE.getString("type"));
							deleteByQueryRequestBuilder.setQuery(query);
							DeleteByQueryResponse response = deleteByQueryRequestBuilder.execute().actionGet();										
							ifUpdated.put(ID, ID);
							//System.out.println("删除DOC数据结束！" + ADDRESS_ID + "<------>" + "response:" + response.status() + "<------>" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
							ifsucess = false;							
						}catch(NoNodeAvailableException E){	
							
					    	client = new TransportClient(settings).addTransportAddress(
					    			new InetSocketTransportAddress(dbconn.RESOURCE_BUNDLE.getString("TransportAddress"),
					    					Integer.parseInt(dbconn.RESOURCE_BUNDLE.getString("TransportAddressPort"))));
						
					    	System.out.println("连接断开重新连接进行操作！<------>" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
					    	continue;					    	
					    }catch(Exception E){					    	
					    	E.printStackTrace();
							System.out.println("删除DOC数据出现异常！" + ID + "<------>" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
					    	break;
					    }
			    	}
				}
								     
				/*更新数据*/
								
				try{
					if (resultSetArea.getInt("ADDR_STATUS_CD") != 2 && resultSetArea.getInt("STATUS_CD") != 2 && !"2".equals(resultSetArea.getString("STATUS"))) {
						count ++;
						Map<String, Object> ret = new HashMap<String, Object>();	
						ret.put("TYCODE", resultSetArea.getString("TYCODE"));
						ret.put("BOOST", resultSetArea.getInt("BOOST"));
						ret.put("NAME", resultSetArea.getString("NAME"));
						ret.put("FULLNAME", resultSetArea.getString("FULLNAME"));
						ret.put("DISPLAYNAME", resultSetArea.getString("DISPLAYNAME"));
						ret.put("ADDRESS_ID", resultSetArea.getString("ADDRESS_ID"));
						ret.put("ADDR_TYPE_CD", resultSetArea.getString("ADDR_TYPE_CD"));
						ret.put("AVG_PRICE", resultSetArea.getInt("AVG_PRICE"));
						ret.put("AREA_ID", resultSetArea.getInt("AREA_ID"));
						ret.put("CITY_ID", resultSetArea.getInt("CITY_ID"));
						ret.put("X", resultSetArea.getFloat("X"));
						ret.put("Y", resultSetArea.getFloat("Y"));
						ret.put("ROAD_NUMBER", resultSetArea.getString("ROAD_NUMBER"));
						ret.put("CLASSIFY", resultSetArea.getInt("CLASSIFY"));
						ret.put("ALLNAME", resultSetArea.getString("ALLNAME"));
						ret.put("FULL_SIMPLE_SPELL", resultSetArea.getString("FULL_SIMPLE_SPELL"));
						ret.put("VERSION", resultSetArea.getTimestamp("VERSION"));
						ret.put("GEOGRAPHY_LOC_ID", resultSetArea.getInt("GEOGRAPHY_LOC_ID"));
						ret.put("UP_GEO_LOC_ID", resultSetArea.getInt("UP_GEO_LOC_ID"));
						ret.put("ID", resultSetArea.getString("ID"));
	                    						
						bulkRequest.add(client.prepareIndex(dbconn.RESOURCE_BUNDLE.getString("index"),dbconn.RESOURCE_BUNDLE.getString("type")).setSource(ret));
						if(count%1000 == 0){
							bulkResponse = bulkRequest.execute().actionGet();
							bulkRequest = client.prepareBulk();
							if(bulkResponse.hasFailures()){
								System.out.println("更新DOC数据出现异常---ADDRESS_ID:" + ID + "<------>" + "bulkResponse:" + bulkResponse.buildFailureMessage() + "<------>" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
			                }
						}
						//System.out.println("新增DOC数据结束！" + ADDRESS_ID+ "<------>" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
					}
				
				}catch(Exception e){
					  e.printStackTrace();
					  System.out.println("更新DOC数据出现异常---ADDRESS_ID:" + ID + "<------>" + "bulkResponse:" + bulkResponse.buildFailureMessage() + "<------>" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                      continue;
				}
			}

			if(count%1000 != 0){ bulkResponse = bulkRequest.execute().actionGet(); bulkRequest = client.prepareBulk();}			
			System.out.println("mergeinto引擎数据结束！"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			
			try {
				generationIncremental.close();
				resultSetArea.close();
				statement.close();
				dbconn.conn.close();
				client.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("更新任务完成，退出时候关闭数据库连接发生异常！");
				e.printStackTrace();
			}
			
		}
	}
}

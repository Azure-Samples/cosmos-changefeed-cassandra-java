package com.azure.cosmosdb.cassandra.examples;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import com.azure.cosmosdb.cassandra.util.CassandraUtils;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example class which will demonstrate following operations on Cassandra Database on CosmosDB
 * - Create Keyspace
 * - Create Table
 * - Insert Rows
 * - Select all data from a table
 * - Select a row from a table
 */
public class UserProfile {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserProfile.class);

    public static void main(String[] s) throws Exception {

        CassandraUtils utils = new CassandraUtils();
        Session cassandraSession = utils.getSession();

        try {
        	  DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");  
        	   LocalDateTime now = LocalDateTime.now().minusHours(6).minusMinutes(30);  
        	   String query="SELECT * FROM uprofile.user where user_id=1 and COSMOS_CHANGEFEED_START_TIME()='" 
           			+ dtf.format(now)+ "'";
        	   
        	 byte[] token=null; 
        	 System.out.println(query); 
        	 while(true)
        	 {
        		 SimpleStatement st=new  SimpleStatement(query);
        		 st.setFetchSize(100);
        		 if(token!=null)
        			 st.setPagingStateUnsafe(token);
        		 
        		 ResultSet result=cassandraSession.execute(st) ;
        		 token=result.getExecutionInfo().getPagingState().toBytes();
        		 
        		 for(Row row:result)
        		 {
        			 System.out.println(row.getString("user_name"));
        		 }
        	 }
                  	

        } finally {
            utils.close();
            LOGGER.info("Please delete your table after verifying the presence of the data in portal or from CQL");
        }
    }
}

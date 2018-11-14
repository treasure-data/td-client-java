package com.treasuredata.client;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;

/**
 * Created by ttruong on 2018-11-12.
 */
public class TDClientQuery
{
    private HashMap parameter;
    private static final Logger logger = LoggerFactory.getLogger(TDClientQuery.class);
    public static final String DB = "-db";
    public static final String TB = "-tb";
    public static final String CL = "-c";
    public static final String MIN = "-m";
    public static final String MAX = "-M";
    public static final String ENG = "-e";
    public static final String LIM = "-l";

    // This option has not been supported yet
    public final String FRM = "-f";

    public TDClientQuery(){
        parameter = new HashMap();
        parameter.put(DB, null);
        parameter.put(TB, null);
        parameter.put(CL, null);
        parameter.put(MIN, null);
        parameter.put(MAX, null);
        parameter.put(ENG, "presto");
        parameter.put(FRM, "tabular");
        parameter.put(LIM, null);
    }

    public void update(HashMap param){
        Set keys = param.keySet();
        Iterator i = keys.iterator();
        while (i.hasNext()){
            Object keyName = i.next();
            parameter.put(keyName, param.get(keyName));
        }
    }

    public void put(String keyName, Object value){
        parameter.put(keyName, value);
    }

    public boolean verifyParameters(){
        if (parameter.get(DB) == null){
            logger.error("Cannot find db_name or its value is null");
            return false;
        }
        if (parameter.get(TB) == null){
            logger.error("Cannot find tb_name or its value is null");
            return false;
        }
        if (parameter.get(MIN) != null && parameter.get(MAX) != null){
            if (Integer.parseInt(parameter.get(MIN).toString()) > Integer.parseInt(parameter.get(MAX).toString())){
                logger.error("Max timestamp is less than Min timestamp");
                return false;
            }
        }
        return true;
    }

    public String createQuery(){
        String query = "";
        if (verifyParameters()){
            query += (parameter.get(CL) == null || parameter.get(CL).toString().trim().isEmpty())? "select * from " : "select " + parameter.get(CL).toString() + " from ";
            query += parameter.get(TB).toString() + " ";
            if (parameter.get(MIN) != null || parameter.get(MAX) != null){
                query += "where ";
                if (parameter.get(MIN) != null)
                    query += "executedend >= " + parameter.get(MIN).toString() + " ";
                if (parameter.get(MAX) != null)
                    if (parameter.get(MIN) != null)
                        query += "and executedend <= " + parameter.get(MAX).toString() + " ";
                    else
                        query += "executedend <= " + parameter.get(MAX).toString() + " ";
            }
            if (parameter.get(LIM) != null)
                query += "limit " + parameter.get(LIM).toString();
        }
        return query;
    }

    public String executeQuery(TDClient client, String query){
        String jobId;
        if (parameter.get(ENG).toString().equalsIgnoreCase("presto"))
            jobId = client.submit(TDJobRequest.newPrestoQuery(parameter.get(DB).toString(), query));
        else {
            jobId = client.submit(TDJobRequest.newHiveQuery(parameter.get(DB).toString(), query));
        }
        return jobId;
    }
}

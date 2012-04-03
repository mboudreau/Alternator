/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michelboudreau.alternator;

import com.michelboudreau.db.Item;
import com.michelboudreau.db.Table;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author thomasbredillet
 */
public class AlternatorDB {

    private List<Table> tables = new ArrayList<Table>();
    Logger logger = LoggerFactory.getLogger(AlternatorDB.class);

    private String oldRequest="";
    
    public AlternatorDB() {
    }

    public Map<String, Object> handleRequest(HttpServletRequest request) {
        try {
            String type = getTypeFromRequest(request);
            String data = getDataFromPost(request);
            ObjectMapper mapper = new ObjectMapper();
            JsonFactory factory = mapper.getJsonFactory();
            JsonParser jp = factory.createJsonParser(data);
            JsonNode actualObj = mapper.readTree(jp);
            if ("put_item".equals(type)) {
                putItem(actualObj);
            } else if ("get_item".equals(type)) {
                return getItem(actualObj);
            } else if ("query".equals(type)) {
            } else if ("scan".equals(type)) {
                return scan(actualObj);
            } else if ("create_table".equals(type)) {
                createTable(actualObj);
            }
            return null;
        } catch (IOException e) {
            logger.debug("request wasn't handled correctly : " + e);
            return null;
        }
    }

    public Map<String, Object> scan(JsonNode data) {
        List<Item> result = new ArrayList<Item>();
        try {
            String tableName = data.path("TableName").getTextValue();
            String limit = null;
            if (!data.path("limit").isNull()) {
                limit = "" + data.path("limit").getIntValue();
            }
            if (!data.path("ScanFilter").isNull()) {
                if (!data.path("ScanFilter").isNull()) {
                    String comparator = data.path("ScanFilter").path("ComparisonOperator").getTextValue();
                    String rangeKey = tableGetRangeKey(tableName);
                    if ("BETWEEN".equals(comparator)) {
                        String lowerBound = data.path("ScanFilter").path(rangeKey).path("AttributeValueList").path(0).getTextValue();
                        String upperBound = data.path("ScanFilter").path(rangeKey).path("AttributeValueList").path(1).getTextValue();
                        for (Item itm : getTable(tableName).getItems()) {
//                           if ((lowerBound.compareTo(itm.getAttributes().get(itm.getRangeKey())) < 0) && (upperBound.compareTo(itm.getAttributes().get(itm.getRangeKey())) > 0)) {
//                                result.add(itm);
//                            }
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            logger.debug("table wasn't created correctly : " + e);
        }
        return null;
    }

    public void createTable(JsonNode data) {

        JsonNode actualObj = data;
        String tableName = actualObj.path("TableName").getTextValue();
        String hashKey = null;
        String rangeKey = null;

        hashKey = actualObj.path("KeySchema").path("HashKeyElement").path("AttributeName").getTextValue();
        if (!actualObj.path("KeySchema").path("RangeKeyElement").path("AttributeName").isNull()) {
            rangeKey = actualObj.path("KeySchema").path("RangeKeyElement").path("AttributeName").getTextValue();
        }
        if (getTable(tableName) == null) {
            Table table = new Table(hashKey, rangeKey, tableName);
            getTables().add(table);
        }
    }

    public void putItem(JsonNode data) {
        try {
            JsonNode actualObj = data;
            String tableName = actualObj.path("TableName").getTextValue();
            Iterator itr = actualObj.path("Item").getFieldNames();
            HashMap<String, Map<String, String>> attributes = new HashMap<String, Map<String, String>>();
            while (itr.hasNext()) {
                String attrName = itr.next().toString();
                Map<String, String> schema = new HashMap<String, String>();
                if (!actualObj.path("Item").path(attrName).path("S").isNull()) {
                    schema.put("S", actualObj.path("Item").path(attrName).path("S").getTextValue());
                    attributes.put(attrName, schema);
                } else if (!actualObj.path("Item").path(attrName).path("N").isNull()) {
                    schema.put("N", actualObj.path("Item").path(attrName).path("N").getTextValue());
                    attributes.put(attrName, schema);
                }
            }
            if (getTable(tableName) == null) {
                throw new IOException("table doesn't exist");
            }
            if (findItemByAttributes(attributes, tableName) != null) {
                getTable(tableName).removeItem(findItemByAttributes(attributes, tableName));
            }
            if (attributes != null && !attributes.isEmpty()) {
                Item item = new Item(tableName, tableGetHashKey(tableName), tableGetRangeKey(tableName), attributes);
                getTable(tableName).addItem(item);
            } else {
                throw new IOException("item empty");
            }
        } catch (IOException e) {
            logger.debug("item wasn't put correctly : " + e);
        }
    }

    public Map<String, Object> getItem(JsonNode data) {
        String key = null;
        String tableName = null;
        Map<String, Object> response = new HashMap<String, Object>();

        try {
            JsonNode actualObj = data;
            tableName = actualObj.path("TableName").getTextValue();
            if (!actualObj.path("Key").path("HashKeyElement").path("S").isNull()) {
                key = actualObj.path("Key").path("HashKeyElement").path("S").getTextValue();
            } else if (!actualObj.path("Key").path("HashKeyElement").path("N").isNull()) {
                key = actualObj.path("Key").path("HashKeyElement").path("N").getTextValue();
            }

            if (key == null) {
                throw new IOException("Bad request in getItem");
            }


            if (getTable(tableName) == null) {
                throw new IOException("table doesn't exist");
            }

            response.put("ConsumedCapacityUnits", 1);
            List<Item> items = findItemByKey(key, tableName);
            if (items != null) {
                for (Item itm : items) {
                    response.put("Item", itm.getAttributes());
                }
            }
        } catch (IOException e) {
            logger.debug("item wasn't put correctly : " + e);
        }
        System.out.println(response);
        return response;
    }

    public Table getTable(String tableName) {
        Table result = null;
        int count = 0;
        for (Table table : getTables()) {
            if (tableName.equals(table.getName())) {
                result = table;
                count++;
            }
        }
        if (count > 1) {
            logger.debug("Error several tables with the same name");
        }
        return result;
    }

    public String tableGetHashKey(String tableName) {
        String result = null;
        for (Table table : getTables()) {
            if (tableName.equals(table.getName())) {
                result = table.getHashKey();
            }
        }
        return result;
    }

    public String tableGetRangeKey(String tableName) {
        String result = null;
        for (Table table : getTables()) {
            if (tableName.equals(table.getName())) {
                result = table.getRangeKey();
            }
        }
        return result;
    }

    public Item findItemByAttributes(HashMap<String, Map<String, String>> attr, String tableName) {
        Item result = null;
        for (Table table : getTables()) {
            if (tableName.equals(table.getName())) {
                if (table.getItems() != null) {
                    for (Item itm : table.getItems()) {
                        if (valuesFromAttributes(attr).equals(valuesFromAttributes(itm.getAttributes()))) {
                            result = itm;
                        }
                    }
                }
            }
        }
        return result;
    }

    public List<Item> findItemByKey(String key, String tableName) throws IOException {
        List<Item> result = new ArrayList<Item>();
        for (Table table : getTables()) {
            if (tableName.equals(table.getName())) {
                if (table.getItems() != null) {
                    for (Item itm : table.getItems()) {
                        if (key != null) {
                            if (!itm.getAttributes().get(table.getHashKey()).get("S").isEmpty()) {
                                if (key.equals(itm.getAttributes().get(table.getHashKey()).get("S"))) {
                                    result.add(itm);
                                }
                            } else if (!itm.getAttributes().get(table.getHashKey()).get("N").isEmpty()) {
                                if (key.equals(itm.getAttributes().get(table.getHashKey()).get("N"))) {
                                    result.add(itm);
                                }
                            }
                        } else {
                            throw new IOException("bad requests in findItemByKey");
                        }
                    }
                }
            }
        }
        return result;
    }

    public String getTypeFromRequest(HttpServletRequest req) {
        String type = null;
        if (req.getHeader("X-Amz-Target") != null) {
            Pattern p = Pattern.compile(".([A-Za-z]+)");
            Matcher m = p.matcher(req.getHeader("X-Amz-Target"));
            while (m.find()) {
                type = m.group(1);
            }
        }
        if ("PutItem".equals(type)) {
            type = "put_item";
        } else if ("Query".equals(type)) {
            type = "query";
        } else if ("Scan".equals(type)) {
            type = "scan";
        } else if ("GetItem".equals(type)) {
            type = "get_item";
        } else if ("CreateTable".equals(type)) {
            type = "create_table";
        }

        return type;
    }

    public String getDataFromPost(HttpServletRequest req) {
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader reader = req.getReader();
            reader.mark(10000);

            String line;
            do {
                line = reader.readLine();
                sb.append(line).append("\n");
            } while (line != null);
            reader.reset();
            // do NOT close the reader here, or you won't be able to get the post data twice
        } catch (IOException e) {
            logger.debug("getPostData couldn't.. get the post data");  // This has happened if the request's reader is closed    
        }
        System.out.println("Old Request : " + this.getOldRequest().substring(0, this.getOldRequest().indexOf("\n")));
//        sb.toString().substring(, this.getOldRequest().indexOf(this.getOldRequest());
        this.setOldRequest(sb.toString().replace(this.getOldRequest(), ""));
        System.out.println(this.getOldRequest());
        return this.getOldRequest();
    }

    /**
     * @return the tables
     */
    public List<Table> getTables() {
        return tables;
    }

    /**
     * @param tables the tables to set
     */
    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public String convertor(List<Item> items) {
        String result = "[{";
        for (Item itm : items) {
        }
        return result;
    }

    public HashSet<String> valuesFromAttributes(HashMap<String,Map<String,String>> map){
        HashSet<String> result = new HashSet<String>();
        for(Map<String,String> mp : map.values()){
            for(String attr : mp.values()){
                result.add(attr);
            }
        }
        return result;
    }

    /**
     * @return the oldRequest
     */
    public String getOldRequest() {
        return oldRequest;
    }

    /**
     * @param oldRequest the oldRequest to set
     */
    public void setOldRequest(String oldRequest) {
        this.oldRequest = oldRequest;
    }
}

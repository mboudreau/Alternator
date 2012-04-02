/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michelboudreau.alternator;

import com.michelboudreau.db.Item;
import com.michelboudreau.db.Table;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

    public void handleRequest(HttpServletRequest request) {
        String type = getTypeFromRequest(request);
        String data = getDataFromPost(request);
        if ("put_item".equals(type)) {
            putItem(data);
        } else if ("get_item".equals(type)) {
            getItem(data);
        } else if ("query".equals(type)) {
        } else if ("scan".equals(type)) {
            scan(data);
        } else if ("create_table".equals(type)) {
            createTable(data);
        }
    }

    public List<Item> scan(String data) {
        List<Item> result = new ArrayList<Item>();
        try {
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getJsonFactory();
        JsonParser jp = factory.createJsonParser(data);
        JsonNode actualObj = mapper.readTree(jp);
        String tableName = actualObj.path("TableName").toString();
        String operator = actualObj.path("TableName").toString();
        }catch (IOException e) {
            logger.debug("table wasn't created correctly : " + e);
        }
        return result;
    }

    public void createTable(String data) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonFactory factory = mapper.getJsonFactory();
            JsonParser jp = factory.createJsonParser(data);
            JsonNode actualObj = mapper.readTree(jp);
            String tableName = actualObj.path("TableName").toString();
            String hashKey = null;
            String rangeKey = null;
            if (!actualObj.path("KeySchema").path("HashKeyElement").path("AttributeName").path("S").isNull()) {
                hashKey = actualObj.path("KeySchema").path("HashKeyElement").path("AttributeName").path("S").toString();
            }
            if (!actualObj.path("KeySchema").path("HashKeyElement").path("AttributeName").path("N").isNull()) {
                hashKey = actualObj.path("KeySchema").path("HashKeyElement").path("AttributeName").path("N").toString();
            }
            if (!actualObj.path("KeySchema").path("RangeKeyElement").path("AttributeName").path("S").isNull()) {
                rangeKey = actualObj.path("KeySchema").path("HashKeyElement").path("AttributeName").path("S").toString();
            }
            if (!actualObj.path("KeySchema").path("RangeKeyElement").path("AttributeName").path("N").isNull()) {
                rangeKey = actualObj.path("KeySchema").path("HashKeyElement").path("AttributeName").path("N").toString();
            }
            Table table = new Table(hashKey, rangeKey, tableName);
            tables.add(table);

        } catch (IOException e) {
            logger.debug("table wasn't created correctly : " + e);
        }
    }

    public void putItem(String data) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonFactory factory = mapper.getJsonFactory();
            JsonParser jp = factory.createJsonParser(data);
            JsonNode actualObj = mapper.readTree(jp);
            String tableName = actualObj.path("TableName").toString();
            Iterator itr = actualObj.path("Item").getFieldNames();
            HashMap<String, String> attributes = new HashMap<String, String>();
            while (itr.hasNext()) {
                String attrName = itr.next().toString();
                if (!actualObj.path("Item").path(attrName).path("S").isNull()) {
                    attributes.put(attrName, actualObj.path("Item").path(attrName).path("S").toString());
                } else if (!actualObj.path("Item").path(attrName).path("N").isNull()) {
                    attributes.put(attrName, actualObj.path("Item").path(attrName).path("N").toString());
                }
            }

            if (getTable(tableName) == null) {
                throw new IOException("table doesn't exist");
            }
            if (findItemByAttributes(attributes, tableName) != null) {
                getTable(tableName).removeItem(findItemByAttributes(attributes, tableName));
            }
            Item item = new Item(getTable(tableName), tableGetHashKey(tableName), tableGetRangeKey(tableName), attributes);

        } catch (IOException e) {
            logger.debug("item wasn't put correctly : " + e);
        }
    }

    public Item getItem(String data) {
        String key = null;
        String tableName = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonFactory factory = mapper.getJsonFactory();
            JsonParser jp = factory.createJsonParser(data);
            JsonNode actualObj = mapper.readTree(jp);
            tableName = actualObj.path("TableName").toString();
            if (!actualObj.path("Key").path("HasKeyElement").path("S").isNull()) {
                key = actualObj.path("Key").path("HasKeyElement").path("S").toString();
            } else if (!actualObj.path("Key").path("HashKeyElement").path("N").isNull()) {
                key = actualObj.path("Key").path("HasKeyElement").path("N").toString();
            }

            if (getTable(tableName) == null) {
                throw new IOException("table doesn't exist");
            }


        } catch (IOException e) {
            logger.debug("item wasn't put correctly : " + e);
        }
        return findItemByKey(key, tableName);
    }

    public Table getTable(String tableName) {
        Table result = null;
        int count = 0;
        for (Table table : tables) {
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
        for (Table table : tables) {
            if (tableName.equals(table.getName())) {
                result = table.getHashKey();
            }
        }
        return result;
    }

    public String tableGetRangeKey(String tableName) {
        String result = null;
        for (Table table : tables) {
            if (tableName.equals(table.getName())) {
                result = table.getRangeKey();
            }
        }
        return result;
    }

    public Item findItemByAttributes(HashMap<String, String> attr, String tableName) {
        Item result = null;
        for (Table table : tables) {
            if (tableName.equals(table.getName())) {
                for (Item itm : table.getItems()) {
                    if (itm.getAttributes() == attr) {
                        result = itm;
                    }
                }
            }
        }
        return result;
    }

    public Item findItemByKey(String key, String tableName) {
        Item result = null;
        for (Table table : tables) {
            if (tableName.equals(table.getName())) {
                for (Item itm : table.getItems()) {
                    if (itm.getAttributes().get(table.getHashKey()) == key) {
                        result = itm;
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
            System.out.println("getPostData couldn't.. get the post data");  // This has happened if the request's reader is closed    
        }

        return sb.toString();
    }
}

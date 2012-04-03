package com.michelboudreau.controller;

import com.michelboudreau.alternator.AlternatorDB;

import com.michelboudreau.db.Table;
import java.io.File;


import java.io.IOException;
import java.util.List;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.http.HttpStatus;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping(value = "/", produces = "application/json")
public class WebController {

    private AlternatorDB db = new AlternatorDB();
    private String dbName = "alternator.db";
    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(method = RequestMethod.POST, consumes = "application/x-amz-json-1.0")
    public @ResponseBody Map<String, Object> alternatorDBController(HttpServletRequest request) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        if (new File(dbName).exists()) {
            this.db = mapper.readValue(new File(dbName), AlternatorDB.class);
//           JsonNode nodes = mapper.readTree(new File("test.json"));
//           for(JsonNode node : nodes){
//               System.out.println(mapper.readValue(node, Table.class).toString());
//           }
//           
//           System.out.println(nodes.toString());
//           for(Object tbl : tables){
//               System.out.println(tbl.toString());
//               
//               Table table = mapper.readValue(tbl.toString(), Table.class);
//               System.out.println(table.getName());
//           }
        }
        System.out.println(request);

        Map<String, Object> response = db.handleRequest(request);

        mapper.writeValue(new File(dbName), db);
        return response;



        //String res = "{\"Count\" :1,\"Items\":[{\"activityType\": {\"S\":\"volleyball\"},\"eventTimeRange\": {\"S\":\"1333494000000\"},\"description\": {\"S\":\"bouboup\"},\"id\": {\"S\":\"MU-djrmrcyqgbfb\"},\"name\": {\"S\":\"Intermediate Tuesday Volleyball\"},\"venue_address1\": {\"S\":\"235 East 49th Street\"},\"venue_address2\": {\"S\":\"Basement\"},\"venue_address3\": {\"S\":\"null\"},\"venue_city\": {\"S\":\"New York\"},\"venue_state\": {\"S\":\"NY\"},\"venue_country\": {\"S\":\"us\"},\"venue_lat\": {\"N\":40.75482},\"venue_lon\": {\"N\":-73.969696},\"venue_name\": {\"S\":\"CATSChildrens Athletic  Training School\"},\"venue_phone\": {\"S\":\"212-751-4876\"},\"venue_zip\": {\"S\":\"10017\"},\"meetupUrl\": {\"S\":\"htal\"} }],\"ConsumedCapacityUnits\":1}";

//        Map<String, String> keys = new HashMap<String, String>();
//        keys.put("S", "test");
//
//        Map<String, Object> res = new HashMap<String, Object>();
//        res.put("Count", 2);
//        res.put("ScannedCount", 2);
//        res.put("ConsumedCapacityUnits", 1);
//        Map<String, Object> item = new HashMap<String, Object>();
//        item.put("Description", keys);
//        item.put("Name", keys);
//        Map<String, Object> item2 = new HashMap<String, Object>();
//        item2.put("Description", keys);
//        item2.put("Name", keys);
//        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
//        list.add(item);
//        list.add(item2);
//        res.put("Items", list);
//
//        Map<String, Object> key = new HashMap<String, Object>();
//        key.put("HashKeyElement", keys);
//        key.put("RangeKeyElement", keys);
//        res.put("LastEvaluatedKey", key);

//                String res = "{\"ConsumedCapacityUnits\":1,\"Item\":{\"de\":{\"S\":\"afaf\"},\"n\":{\"S\":\"fafa\"},\"etr\":{\"S\":\"adad\"},\"ty\":{\"S\":\"ad\"},\"id\":{\"S\":\"sefsef\"}}}";
//
//        return res;
    }

    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(value = "/tables", method = RequestMethod.GET)
    public @ResponseBody
    Iterable<Table> getTables() {
        return db.getTables();
    }
    
     @ResponseStatus(HttpStatus.OK)
    @RequestMapping(value = "/db", method = RequestMethod.GET)
    public @ResponseBody AlternatorDB getData() {
        return db;
    }
//    @ResponseStatus(HttpStatus.OK)
//    @RequestMapping(value = "/item", method = RequestMethod.GET)
//    public @ResponseBody
//    String getItems(@RequestParam(required = true) String tablename) {
//        System.out.println("getItems");
//        System.out.println("Tablename : " + tablename);
//        if (db.getTable(tablename) != null) {
//            for (Item itm : db.getTable(tablename).getItems()) {
//                System.out.println("----next item----");
//                System.out.println(itm.toString());
//            }
//
//        } else {
//            System.out.println("table is empty");
//        }
////                String res = "{\"Count\" :1,\"Items\":[{\"activityType\": {\"S\":\"volleyball\"},\"eventTimeRange\": {\"S\":\"1333494000000\"},\"description\": {\"S\":\"bouboup\"},\"id\": {\"S\":\"MU-djrmrcyqgbfb\"},\"name\": {\"S\":\"Intermediate Tuesday Volleyball\"},\"venue_address1\": {\"S\":\"235 East 49th Street\"},\"venue_address2\": {\"S\":\"Basement\"},\"venue_address3\": {\"S\":\"null\"},\"venue_city\": {\"S\":\"New York\"},\"venue_state\": {\"S\":\"NY\"},\"venue_country\": {\"S\":\"us\"},\"venue_lat\": {\"N\":40.75482},\"venue_lon\": {\"N\":-73.969696},\"venue_name\": {\"S\":\"CATSChildrens Athletic  Training School\"},\"venue_phone\": {\"S\":\"212-751-4876\"},\"venue_zip\": {\"S\":\"10017\"},\"meetupUrl\": {\"S\":\"htal\"} }],\"ConsumedCapacityUnits\":1}";
//
////        Map<String, String> keys = new HashMap<String, String>();
////        keys.put("S", "test");
////
////        Map<String, Object> res = new HashMap<String, Object>();
////        res.put("Count", 2);
////        res.put("ScannedCount", 2);
////        res.put("ConsumedCapacityUnits", 1);
////        Map<String, Object> item = new HashMap<String, Object>();
////        item.put("Description", keys);
////        item.put("Name", keys);
////        Map<String, Object> item2 = new HashMap<String, Object>();
////        item2.put("Description", keys);
////        item2.put("Name", keys);
////        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
////        list.add(item);
////        list.add(item2);
////        res.put("Items", list);
////
////        Map<String, Object> key = new HashMap<String, Object>();
////        key.put("HashKeyElement", keys);
////        key.put("RangeKeyElement", keys);
////        res.put("LastEvaluatedKey", key);
//
//        String res = "{\"ConsumedCapacityUnits\":1,\"Item\":{\"de\":{\"S\":\"afaf\"},\"n\":{\"S\":\"fafa\"},\"etr\":{\"S\":\"adad\"},\"ty\":{\"S\":\"ad\"},\"id\":{\"S\":\"sefsef\"}}}";
//        return res;
//    }
}

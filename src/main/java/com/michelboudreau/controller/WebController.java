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
        }
        System.out.println(request);

        Map<String, Object> response = db.handleRequest(request);

        mapper.writeValue(new File(dbName), db);
        return response;
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
}

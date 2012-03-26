package com.michelboudreau.controller;


import com.michelboudreau.db.Element;
import com.michelboudreau.db.Table;
import javax.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;



@Controller
@RequestMapping(value="/", produces="application/json")
public class WebController {


    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(method=RequestMethod.GET)
    public void getFromDB(HttpServletRequest request) {
        System.out.println("GET");
    }
    
    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(method=RequestMethod.PUT)
    public void putInDB(HttpServletRequest request) {
        System.out.println("PUT");
    }
    
    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(method=RequestMethod.DELETE)
    public void deleteFromDB() {
        System.out.println("DELETE");
    }
    
    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(method=RequestMethod.POST)
    public void deleteFromDB1() {
        Table tbl = null;

        System.out.println("POST");
    }
}

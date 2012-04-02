package com.michelboudreau.controller;

import com.michelboudreau.alternator.AlternatorDB;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import javax.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@Controller
@RequestMapping(value = "/", consumes = "application/x-amz-json-1.0", produces = "application/json")
public class WebController {

    private AlternatorDB db = new AlternatorDB();

    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(method = RequestMethod.POST, consumes = "application/x-amz-json-1.0")
    public void alternatorDBController(HttpServletRequest request) throws IOException {
        db.handleRequest(request);
        System.out.println(db.getDataFromPost(request));
        System.out.println(db.getTypeFromRequest(request));
    }
}

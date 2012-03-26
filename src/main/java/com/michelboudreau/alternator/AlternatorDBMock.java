/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michelboudreau.alternator;

import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMappingException;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodb.model.*;
import com.michelboudreau.db.Element;
import com.michelboudreau.db.Table;
import java.lang.reflect.Method;
import java.util.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJacksonHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author thomasbredillet
 */
public class AlternatorDBMock  {

    private List<Table> tables;
    
 
    
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michelboudreau.alternator;

import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.datamodeling.*;
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
public class AlternatorDBMapper extends com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper {
    private AlternatorDBMock db;
   
    public AlternatorDBMapper(AmazonDynamoDB dynamoDB) {
        super(dynamoDB, DynamoDBMapperConfig.DEFAULT);
    }

    @Override
    public <T extends Object> void save(T object, DynamoDBMapperConfig config) {        
        //db.
    }
    
    @Override
    public <T extends Object> T load(Class<T> clazz, Object hashKey, DynamoDBMapperConfig config) {
        return load(clazz, hashKey, null, config);
    }

    @Override
    public <T extends Object> T load(Class<T> clazz, Object hashKey) {
        return load(clazz, hashKey, null, null);
    }

    @Override
    public <T extends Object> T load(Class<T> clazz, Object hashKey, Object rangeKey) {
        return load(clazz, hashKey, rangeKey, null);
    }

    @Override
    public <T extends Object> T load(Class<T> clazz, Object hashKey, Object rangeKey, DynamoDBMapperConfig config) {
        T obj = null;
        return obj;
    }
    
    @Override
   public <T> PaginatedQueryList<T> query(Class<T> clazz, DynamoDBQueryExpression queryExpression) {
        return query(clazz, queryExpression, null);
    }
    
    @Override
    public <T> PaginatedQueryList<T> query(Class<T> clazz, DynamoDBQueryExpression queryExpression, DynamoDBMapperConfig config) {
        PaginatedQueryList<T> list = null;
        return list;
    }
    

}

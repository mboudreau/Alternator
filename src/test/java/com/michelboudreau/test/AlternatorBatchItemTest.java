package com.michelboudreau.test;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.dynamodb.model.*;
import com.amazonaws.services.dynamodb.model.transform.UpdateItemRequestJsonUnmarshaller;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.Unmarshaller;
import com.michelboudreau.alternator.enums.AttributeValueType;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext.xml"})
public class AlternatorBatchItemTest extends AlternatorTest {

    private String tableName;

  /*  @Before
    public void setUp() throws Exception {
        tableName = createTableName();
    }

    @After
    public void tearDown() throws Exception {
        deleteAllTables();
    }*/

	@Test
	public void vanillaBatchGetItemTest(){
		BatchGetItemResult result  = client.batchGetItem(new BatchGetItemRequest());
	}

}

package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.model.*;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    /*
    @Test
    public void batchGetItemInTableTest() {
        BatchGetItemResult result = client.batchGetItem(new BatchGetItemRequest());
        Assert.assertNotNull(result);
    }
    */



}

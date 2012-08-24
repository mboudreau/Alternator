package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodb.model.BatchGetItemResult;
import com.amazonaws.services.dynamodb.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodb.model.BatchWriteItemResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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

    @Test
    public void vanillaBatchWriteItemTest(){
        BatchWriteItemResult result  = client.batchWriteItem(new BatchWriteItemRequest());
    }


}

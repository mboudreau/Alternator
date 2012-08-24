package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.model.*;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
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

//	@Test
//	public void vanillaBatchGetItemTest() {
//		BatchGetItemResult result  = client.batchGetItem(new BatchGetItemRequest());
//	}

    @Test
    public void vanillaBatchWriteItemTest() {
        BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest();
        BatchWriteItemResult result;

        // Create a map for the requests in the batch
        Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();

        // Create a PutRequest for a new Forum item
        Map<String, AttributeValue> forumItem = new HashMap<String, AttributeValue>();
        forumItem.put("range", new AttributeValue().withN("1"));
        forumItem.put("range", new AttributeValue().withN("2"));
        forumItem.put("range", new AttributeValue().withN("3"));
        forumItem.put("range", new AttributeValue().withN("4"));
        forumItem.put("range", new AttributeValue().withN("5"));
        forumItem.put("range", new AttributeValue().withN("6"));
        forumItem.put("range", new AttributeValue().withN("7"));
        forumItem.put("range", new AttributeValue().withN("8"));


        List<WriteRequest> forumList = new ArrayList<WriteRequest>();
        forumList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(forumItem)));

        do {
            System.out.println("Making the request.");

            batchWriteItemRequest.withRequestItems(requestItems);
            result = client.batchWriteItem(batchWriteItemRequest);

            // Print consumed capacity units
            for(Map.Entry<String, BatchWriteResponse> entry : result.getResponses().entrySet()) {
                String tableName = entry.getKey();
                Double consumedCapacityUnits = entry.getValue().getConsumedCapacityUnits();
                System.out.println("Consumed capacity units for table " + tableName + ": " + consumedCapacityUnits);
            }

            // Check for unprocessed keys which could happen if you exceed provisioned throughput
            System.out.println("Unprocessed Put and Delete requests: \n" + result.getUnprocessedItems());
            requestItems = result.getUnprocessedItems();
        } while (result.getUnprocessedItems().size() > 0);

    /*
    @Test
    public void batchGetItemInTableTest() {
        BatchGetItemResult result = client.batchGetItem(new BatchGetItemRequest());
        Assert.assertNotNull(result);
>>>>>>> faa37598a99d659e8db521049130d600e03df7fb
    }
    */


    }
}

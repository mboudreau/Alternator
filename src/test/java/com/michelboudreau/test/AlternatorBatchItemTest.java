package com.michelboudreau.test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.*;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext.xml"})
public class AlternatorBatchItemTest extends AlternatorTest {

    private String tableName1;
    private String tableName2;
    private String hashKeyValue1;
    private String hashKeyValue2;

    @Before
    public void setUp() throws Exception {
        tableName1 = createTableName();
        TableDescription tableDescription1 = createTable(tableName1);
        hashKeyValue1 = tableDescription1.getKeySchema().getHashKeyElement().getAttributeName();
        
        tableName2 = createTableName();
        TableDescription tableDescription2 = createTable(tableName2);
        hashKeyValue2 = tableDescription2.getKeySchema().getHashKeyElement().getAttributeName();
    }

    @After
    public void tearDown() throws Exception {
        deleteAllTables();
    }

	@Test
	public void vanillaBatchGetItemTest() throws Exception {
        this.vanillaBatchWriteItemTest();
        BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest();
        Map<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();
        KeysAndAttributes keysAndAttributes = new KeysAndAttributes();
        List<String> attributesToGet = new ArrayList<String>();
        attributesToGet.add(hashKeyValue1);
        keysAndAttributes.setAttributesToGet(attributesToGet);
        List<Key> keys = new ArrayList<Key>();

        KeysAndAttributes keysAndAttributes1 = new KeysAndAttributes();
        List<String> attributesToGet1 = new ArrayList<String>();
        attributesToGet1.add(hashKeyValue2);
        keysAndAttributes1.setAttributesToGet(attributesToGet1);
        //Test case 1: Every request has matches.
//        keys.add(new Key(new AttributeValue("4")));
//        keys.add(new Key(new AttributeValue("5")));
//        keys.add(new Key(new AttributeValue("3")));

        //Test case 2: Requests has no match.
        keys.add(new Key(new AttributeValue("1")));

        //Test case 3: Complicated test, some requests has matches, some doesn't.
//        keys.add(new Key(new AttributeValue("7")));
//        keys.add(new Key(new AttributeValue("4")));

        //Test case 4: Duplicated request
        //Duplicated requests return duplicated results.
//        keys.add(new Key(new AttributeValue("7")));
//        keys.add(new Key(new AttributeValue("7")));
//        keys.add(new Key(new AttributeValue("4")));
//        keys.add(new Key(new AttributeValue("4")));

        keysAndAttributes.setKeys(keys);
        keysAndAttributes1.setKeys(keys);
        //Test case for Exception: Table doesn't exist.
//        requestItems.put("Vito's Table", keysAndAttributes);

        // Normal test
        // TODO: Multi table test failed. Need to be fixed.
        requestItems.put(tableName1, keysAndAttributes);
        requestItems.put(tableName2, keysAndAttributes1);

        batchGetItemRequest.withRequestItems(requestItems);
		BatchGetItemResult result  = client.batchGetItem(batchGetItemRequest);

	}

    @Test
    public void vanillaBatchWriteItemTest() throws Exception{
        BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest();
        BatchWriteItemResult result;

        // Create a map for the requests in the batch
        Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();

        // Test: write items to database
        Map<String, AttributeValue> forumItem = new HashMap<String, AttributeValue>();
        forumItem.put(hashKeyValue1, new AttributeValue().withN("1"));
        forumItem.put("range", new AttributeValue().withS("a"));
        List<WriteRequest> forumList = new ArrayList<WriteRequest>();
        forumList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(forumItem)));

        Map<String, AttributeValue> forumItem1 = new HashMap<String, AttributeValue>();
        forumItem1.put(hashKeyValue1, new AttributeValue().withN("2"));
        forumItem1.put("range", new AttributeValue().withS("b"));
        forumList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(forumItem1)));

        Map<String, AttributeValue> forumItem5 = new HashMap<String, AttributeValue>();
        forumItem5.put(hashKeyValue1, new AttributeValue().withN("3"));
        forumItem5.put("range", new AttributeValue().withS("c"));
        forumList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(forumItem5)));

        Map<String, AttributeValue> forumItem2 = new HashMap<String, AttributeValue>();
        forumItem2.put(hashKeyValue1, new AttributeValue().withN("4"));
        forumItem2.put("range", new AttributeValue().withS("d"));
        forumList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(forumItem2)));

        Map<String, AttributeValue> forumItem3 = new HashMap<String, AttributeValue>();
        forumItem3.put(hashKeyValue1, new AttributeValue().withN("5"));
        forumItem3.put("range", new AttributeValue().withS("e"));
        forumList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(forumItem3)));

        Map<String, AttributeValue> forumItem4 = new HashMap<String, AttributeValue>();
        forumItem4.put(hashKeyValue1, new AttributeValue().withN("6"));
        forumItem4.put("range", new AttributeValue().withS("f"));
        forumList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(forumItem4)));

        //Test case: with duplicated hashkey item but distinguished range key input.
        Map<String, AttributeValue> forumItem6 = new HashMap<String, AttributeValue>();
        forumItem6.put(hashKeyValue1, new AttributeValue().withN("6"));
        forumItem6.put("range", new AttributeValue().withS("ff"));
        forumList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(forumItem6)));

        //Test case: Delete Request
//        forumList.add(new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(new Key(new AttributeValue("7")))));
//        forumList.add(new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(new Key(new AttributeValue("1")))));
//        forumList.add(new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(new Key(new AttributeValue("4")))));
//        forumList.add(new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(new Key(new AttributeValue("5")))));

        //Test case: Duplicated delete request
//        forumList.add(new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(new Key(new AttributeValue("7")))));

        //Test on Table 2
        Map<String, AttributeValue> forumItemT2 = new HashMap<String, AttributeValue>();
        forumItemT2.put(hashKeyValue2, new AttributeValue().withN("1"));
        forumItemT2.put("range", new AttributeValue().withS("a"));
        List<WriteRequest> forumListT2 = new ArrayList<WriteRequest>();
        forumListT2.add(new WriteRequest().withPutRequest(new PutRequest().withItem(forumItemT2)));

        requestItems.put(tableName1, forumList);
        requestItems.put(tableName2, forumListT2);
        do {
            System.out.println("Making the request.");

            batchWriteItemRequest.withRequestItems(requestItems);
            result = client.batchWriteItem(batchWriteItemRequest);

            // Print consumed capacity units
            for(Map.Entry<String, BatchWriteResponse> entry : result.getResponses().entrySet()) {
                String tableName1 = entry.getKey();
                Double consumedCapacityUnits = entry.getValue().getConsumedCapacityUnits();
                System.out.println("Consumed capacity units for table " + tableName1 + ": " + consumedCapacityUnits);
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
    }
    */


    }
}

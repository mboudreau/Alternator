package com.michelboudreau.testv2;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.Unmarshaller;
import com.michelboudreau.alternator.enums.AttributeValueType;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext.xml"})
public class AlternatorItemTest extends AlternatorTest {

    private String tableName;

    @Before
    public void setUp() throws Exception {
        tableName = createTableName();
    }

    @After
    public void tearDown() throws Exception {
        deleteAllTables();
    }

    //Test: put item with HashKey
    @Test
    public void putItemWithHashKey() {
        createGenericTable(tableName);

        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(createGenericItem());
        PutItemResult res = getClient().putItem(request);

        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getConsumedCapacity());
        Assert.assertEquals(tableName, res.getConsumedCapacity().getTableName());
        Assert.assertNotNull(res.getConsumedCapacity().getCapacityUnits());
    }


    @Test
    public void putItemWithHashKeyOverwriteItem() {
        createGenericTable(tableName);

        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(createGenericItem());
        getClient().putItem(request); // put item beforehand
        PutItemResult res = getClient().putItem(request); // Add another

        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getConsumedCapacity());
        Assert.assertEquals(tableName, res.getConsumedCapacity().getTableName());
        Assert.assertNotNull(res.getConsumedCapacity().getCapacityUnits());
    }

    @Test
    public void putItemWithHashKeyWithoutItem() {
        createGenericTable(tableName);

        PutItemRequest request = new PutItemRequest().withTableName(tableName);
		try {
			getClient().putItem(request);
			Assert.assertTrue(false);// Should have thrown an exception
		} catch (AmazonServiceException ase) {
		}
    }

    @Test
    public void putItemWithHashKeyWithoutTableName() {
        createGenericTable(tableName);

        PutItemRequest request = new PutItemRequest().withItem(createGenericItem());
		try {
			getClient().putItem(request);
			Assert.assertTrue(false);// Should have thrown an exception
		} catch (AmazonServiceException ase) {
		}
    }

    //Test: put item with HashKey and RangeKey
    @Test
    public void putItemWithHashKeyAndRangeKey() {
        createGenericHashRangeTable(tableName);

        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(createGenericItem());
        PutItemResult res = getClient().putItem(request);

        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getConsumedCapacity());
        Assert.assertEquals(tableName, res.getConsumedCapacity().getTableName());
        Assert.assertNotNull(res.getConsumedCapacity().getCapacityUnits());
    }

    @Test
    public void putItemWithHashKeyAndRangeKeyOverwriteItem() {
        createGenericHashRangeTable(tableName);

        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(createGenericItem());
        getClient().putItem(request); // put item beforehand
        PutItemResult res = getClient().putItem(request); // Add another
        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getConsumedCapacity());
        Assert.assertEquals(tableName, res.getConsumedCapacity().getTableName());
        Assert.assertNotNull(res.getConsumedCapacity().getCapacityUnits());
    }

    @Test
    public void putItemWithHashKeyAndRangeKeyWithoutItem() {
        createGenericHashRangeTable(tableName);

        PutItemRequest request = new PutItemRequest().withTableName(tableName);
		try {
			getClient().putItem(request);
			Assert.assertTrue(false);// Should have thrown an exception
		} catch (AmazonServiceException ase) {
		}
    }

    @Test
    public void putItemWithHashKeyAndRangeKeyWithoutTableName() {
        createGenericHashRangeTable(tableName);

        PutItemRequest request = new PutItemRequest().withItem(createGenericItem());
		try {
			getClient().putItem(request);
			Assert.assertTrue(false);// Should have thrown an exception
		} catch (AmazonServiceException ase) {
		}
    }

    //---------------------------------------------------------------------------

    // TODO: test out put item expected and return value
    @Test
    public void getItemTest() {
        AttributeValue hash = createItem(tableName);

        GetItemRequest request = new GetItemRequest().withTableName(tableName);
        request.setKey(createItemKey("id", hash));
        GetItemResult res = getClient().getItem(request);

        Assert.assertNotNull(res.getItem());
        Assert.assertEquals(res.getItem().get("id"), hash);
    }

    @Test
    public void getItemWithoutTableNameTest() {
        GetItemRequest request = new GetItemRequest();
        request.setKey(createItemKey("id", new AttributeValue().withNS("123")));
		try {
			getClient().getItem(request);
			Assert.assertTrue(false);// Should have thrown an exception
		} catch (AmazonServiceException ase) {
		}
    }

    @Test
    public void getItemWithoutKeyTest() {
        GetItemRequest request = new GetItemRequest();
        request.setTableName(tableName);
		try {
			getClient().getItem(request);
			Assert.assertTrue(false);// Should have thrown an exception
		} catch (AmazonServiceException ase) {
		}
    }

    @Test
    public void updateItemInTableTest() {
        AttributeValue hash = putItemInDb();
        Map<String, AttributeValueUpdate> attrToUp = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = new AttributeValueUpdate();
        update.setAction("PUT");
        update.setValue(hash);
        attrToUp.put("updated", update);
        UpdateItemRequest request = new UpdateItemRequest(tableName, createItemKey("id", hash), attrToUp);
        UpdateItemResult res = getClient().updateItem(request);
        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getAttributes());
    }

    @Test
    public void updateItemWithoutTableNameTest() {
        AttributeValue hash = putItemInDb();
        Map<String, AttributeValueUpdate> attrToUp = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = new AttributeValueUpdate();
        update.setAction("PUT");
        update.setValue(hash);
        attrToUp.put("updated", update);
        UpdateItemRequest request = new UpdateItemRequest();
        request.setKey(createItemKey("id", hash));
        request.setAttributeUpdates(attrToUp);
		try {
			getClient().updateItem(request);
			Assert.assertTrue(false);// Should have thrown an exception
		} catch (AmazonServiceException ase) {
		}
    }

    @Test
    public void updateItemWithoutKeyTest() {
        AttributeValue hash = putItemInDb();
        Map<String, AttributeValueUpdate> attrToUp = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = new AttributeValueUpdate();
        update.setAction("PUT");
        update.setValue(hash);
        attrToUp.put("updated", update);
        UpdateItemRequest request = new UpdateItemRequest();
        request.setTableName(tableName);
        request.setAttributeUpdates(attrToUp);
		try {
			getClient().updateItem(request);
			Assert.assertTrue(false);// Should have thrown an exception
		} catch (AmazonServiceException ase) {
		}
    }

     @Test
    public void deleteItem() {
        createGenericTable(tableName);

        AttributeValue hash = new AttributeValue("ad"); //createStringAttribute();
        getClient().putItem(new PutItemRequest().withTableName(tableName).withItem(createGenericItem(hash)));
        DeleteItemRequest request = new DeleteItemRequest().withTableName(tableName).withKey(createItemKey("id", hash));
        DeleteItemResult res = getClient().deleteItem(request);

        Assert.assertNotNull(res.getConsumedCapacity());
        Assert.assertEquals(tableName, res.getConsumedCapacity().getTableName());
        Assert.assertNotNull(res.getConsumedCapacity().getCapacityUnits());
    }

    @Test
    public void deleteItemWithHashKeyAndRangeKey() {
        AttributeDefinition hashAttr = createStringAttributeDefinition("id");
        AttributeDefinition rangeAttr = createNumberAttributeDefinition("range");
        createTable(tableName, hashAttr, rangeAttr);

        AttributeValue hash = new AttributeValue("ad");
        AttributeValue range1 = new AttributeValue().withN("1");
        AttributeValue range2 = new AttributeValue().withN("2");
        getClient().putItem(new PutItemRequest().withTableName(tableName).withItem(createGenericItem(hash, range1)));
        getClient().putItem(new PutItemRequest().withTableName(tableName).withItem(createGenericItem(hash, range2)));

        DeleteItemRequest request1 = new DeleteItemRequest().withTableName(tableName).withKey(createItemKey("id", hash, "range", range1)).withReturnValues(ReturnValue.ALL_OLD);
        DeleteItemRequest request2 = new DeleteItemRequest().withTableName(tableName).withKey(createItemKey("id", hash, "range", range2)).withReturnValues(ReturnValue.ALL_OLD);
        DeleteItemResult result = getClient().deleteItem(request1);

        Assert.assertNotNull(result.getAttributes());

        result = getClient().deleteItem(request2);
        Assert.assertNotNull(result.getAttributes());
    }

    @Test
    public void deleteItemWithoutTableName() {
        createGenericTable(tableName);

        AttributeValue hash = createStringAttribute();
        getClient().putItem(new PutItemRequest().withTableName(tableName).withItem(createGenericItem(hash)));
        DeleteItemRequest request = new DeleteItemRequest().withKey(createItemKey("id", hash));

		try {
			getClient().deleteItem(request);
			Assert.assertTrue(false);// Should have thrown an exception
		} catch (AmazonServiceException ase) {
		}
    }

    @Test
    public void deleteNonExistentItem() {
        createGenericTable(tableName);

        DeleteItemRequest request = new DeleteItemRequest().withTableName(tableName).withKey(createItemKey("id", createStringAttribute()));
		try {
			getClient().deleteItem(request);
			Assert.assertTrue(false);// Should have thrown an exception
		} catch (AmazonServiceException ase) {
		}
    }

	@Test
	public void putItemWithExpected() {
        createGenericTable(tableName);

		AttributeValue value = new AttributeValue("test1");
		Map<String, ExpectedAttributeValue> expectedMap = new HashMap<String, ExpectedAttributeValue>();
		expectedMap.put("id", new ExpectedAttributeValue(false));

		Map<String, AttributeValue> item = createGenericItem(value, null);

		AmazonDynamoDB client = getClient();
		client.putItem(new PutItemRequest(tableName, item).withExpected(expectedMap));

		try {
			client.putItem(new PutItemRequest(tableName, item).withExpected(expectedMap));
			Assert.assertTrue(false);// Should have thrown a ConditionalCheckFailedException
		} catch (ConditionalCheckFailedException ccfe) {
		}
	}

    // TODO: test out delete item expected and return value

    /*@Test
    public void batchWriteItemInTableTest() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest();
        Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();
        WriteRequest writeRequest = new WriteRequest();
        writeRequest.setPutRequest(new PutRequest());
        writeRequest.setDeleteRequest(new DeleteRequest());
        List<WriteRequest> writeRequests = new ArrayList<WriteRequest>();
        writeRequests.add(writeRequest);
        requestItems.put(tableName, writeRequests);
        batchWriteItemRequest.setRequestItems(requestItems);
        BatchWriteItemResult result = getClient().batchWriteItem(batchWriteItemRequest);
        Assert.assertNotNull(result);
    }*/
/*
	@Test
	public void batchWriteItemInTableTest() {
		BatchWriteItemResult result = getClient().batchWriteItem(generateWriteBatchRequest());
		Assert.assertNotNull(result);
	}

	@Test
	public void batchGetItemInTableTest() {
		BatchGetItemResult result = getClient().batchGetItem(generateGetBatchRequest());
		Assert.assertNotNull(result);
	}

	@Test
	public void batchGetItemInTableWithoutKeyTest() {
		BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest();
		Map<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();
		requestItems.put(testTableName, null);
		batchGetItemRequest.withRequestItems(requestItems);
		Assert.assertNull(getClient().batchGetItem(batchGetItemRequest).getResponses());
	}

	@Test
	public void batchGetItemInTableWithoutNameTest() {
		BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest();
		Map<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();
		Key table1key1 = new Key().withHashKeyElement(new AttributeValue().withS("123"));
		requestItems.put(null, new KeysAndAttributes().withKeys(table1key1));
		batchGetItemRequest.withRequestItems(requestItems);
		Assert.assertNull(getClient().batchGetItem(batchGetItemRequest).getResponses());
	}

	@Test
	public void batchGetItemInTableWithoutRequestItemsTest() {
		Assert.assertNull(getClient().batchGetItem(new BatchGetItemRequest()).getResponses());
	}
*/

    /*
     public BatchGetItemRequest generateGetBatchRequest() {
         BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest();
         Map<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();
         Key table1key1 = new Key().withHashKeyElement(new AttributeValue().withS("123"));
         requestItems.put(testTableName, new KeysAndAttributes().withKeys(table1key1));
         batchGetItemRequest.withRequestItems(requestItems);
         return batchGetItemRequest;
     }

     public BatchWriteItemRequest generateWriteBatchRequest() {
         BatchWriteItemRequest request = new BatchWriteItemRequest();
         Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();
         List<WriteRequest> itemList = new ArrayList<WriteRequest>();
         itemList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(generateStaticItem())));
         itemList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(generateNewStaticItem())));
         requestItems.put(testTableName, itemList);
         request.setRequestItems(requestItems);
         return request;
     }*/

    private static String readFileAsString(String filePath)
            throws java.io.IOException{
        StringBuffer fileData = new StringBuffer(1000);
        BufferedReader reader = new BufferedReader(
                new FileReader(filePath));
        char[] buf = new char[1024];
        int numRead=0;
        while((numRead=reader.read(buf)) != -1){
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
            buf = new char[1024];
        }
        reader.close();
        return fileData.toString();
    }

    public <T extends AmazonWebServiceRequest> T getData(Class<T> clazz, Unmarshaller<T, JsonUnmarshallerContext> unmarshaller, String json) {
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory jsonFactory = new JsonFactory();
        if (json != null) {
            try {
                JsonParser jsonParser = jsonFactory.createJsonParser(json);
                try {
                    JsonUnmarshallerContext unmarshallerContext = new JsonUnmarshallerContext(jsonParser);
                    T result = unmarshaller.unmarshall(unmarshallerContext);
                    return result;
                } finally {
                }
                //return mapper.readValue(json, clazz);
            } catch (Exception e) {

            }
        } else {

        }
        return null;
    }

    protected AttributeValueType getAttributeValueType(AttributeValue value) {
        if (value != null) {
            if (value.getN() != null) {
                return AttributeValueType.N;
            } else if (value.getS() != null) {
                return AttributeValueType.S;
            } else if (value.getNS() != null) {
                return AttributeValueType.NS;
            } else if (value.getSS() != null) {
                return AttributeValueType.SS;
            }
        }
        return AttributeValueType.UNKNOWN;
    }


    protected AttributeValue createStringAttribute() {
        return new AttributeValue(UUID.randomUUID().toString());
    }

    protected AttributeValue createNumberAttribute() {
        return new AttributeValue().withN(Math.round(Math.random() * 1000) + "");
    }

    protected Map<String, AttributeValue> createGenericItem() {
        return createGenericItem(createStringAttribute(), createStringAttribute());
    }

    protected Map<String, AttributeValue> createGenericItem(AttributeValue hash) {
        return createGenericItem(hash, createStringAttribute());
    }

    protected Map<String, AttributeValue> createGenericItem(AttributeValue hash, AttributeValue range) {
        Map<String, AttributeValue> map = new HashMap<String, AttributeValue>();
        map.put("id", hash);
        if (range != null) {
            map.put("range", range);
        }
        return map;
    }

    protected AttributeValue putItemInDb() {
        createGenericTable(tableName);

        AttributeValue hash = createStringAttribute();
        Map<String, AttributeValue> item = createGenericItem(hash);
        PutItemRequest req = new PutItemRequest().withTableName(tableName).withItem(item);
        getClient().putItem(req);

        return hash;
    }
}

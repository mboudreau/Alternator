package com.michelboudreau.test;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.dynamodb.model.*;
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
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(createGenericItem());
        PutItemResult res = getClient().putItem(request);
        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getConsumedCapacityUnits());
    }


    @Test
    public void putItemWithHashKeyOverwriteItem() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(createGenericItem());
        getClient().putItem(request); // put item beforehand
        PutItemResult res = getClient().putItem(request); // Add another
        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getConsumedCapacityUnits());
    }

    @Test
    public void putItemWithHashKeyWithoutItem() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        PutItemRequest request = new PutItemRequest().withTableName(tableName);
        PutItemResult res = getClient().putItem(request);
        Assert.assertNull(res.getConsumedCapacityUnits());
    }

    @Test
    public void putItemWithHashKeyWithoutTableName() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        PutItemRequest request = new PutItemRequest().withItem(createGenericItem());
        PutItemResult res = getClient().putItem(request);
        Assert.assertNull(res.getConsumedCapacityUnits());
    }

    //Test: put item with HashKey and RangeKey
    @Test
    public void putItemWithHashKeyAndRangeKey() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        schema.setRangeKeyElement(new KeySchemaElement().withAttributeName("range").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(createGenericItem());
        PutItemResult res = getClient().putItem(request);
        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getConsumedCapacityUnits());
    }

    @Test
    public void putItemWithHashKeyAndRangeKeyOverwriteItem() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        schema.setRangeKeyElement(new KeySchemaElement().withAttributeName("range").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(createGenericItem());
        getClient().putItem(request); // put item beforehand
        PutItemResult res = getClient().putItem(request); // Add another
        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getConsumedCapacityUnits());
    }

    @Test
    public void putItemWithHashKeyAndRangeKeyWithoutItem() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        schema.setRangeKeyElement(new KeySchemaElement().withAttributeName("range").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        PutItemRequest request = new PutItemRequest().withTableName(tableName);
        PutItemResult res = getClient().putItem(request);
        Assert.assertNull(res.getConsumedCapacityUnits());
    }

    @Test
    public void putItemWithHashKeyAndRangeKeyWithoutTableName() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        schema.setRangeKeyElement(new KeySchemaElement().withAttributeName("range").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        PutItemRequest request = new PutItemRequest().withItem(createGenericItem());
        PutItemResult res = getClient().putItem(request);
        Assert.assertNull(res.getConsumedCapacityUnits());
    }

    //---------------------------------------------------------------------------

    // TODO: test out put item expected and return value
    @Test
    public void getItemTest() {
        AttributeValue hash = createItem(tableName);
        GetItemRequest request = new GetItemRequest().withTableName(tableName);
        request.setKey(new Key().withHashKeyElement(hash));
        GetItemResult res = getClient().getItem(request);
        Assert.assertNotNull(res.getItem());
        Assert.assertEquals(res.getItem().get("id"), hash);
    }

    @Test
    public void getItemWithoutTableNameTest() {
        GetItemRequest request = new GetItemRequest();
        request.setKey(new Key().withHashKeyElement(new AttributeValue().withNS("123")));
        GetItemResult res = getClient().getItem(request);
        Assert.assertNull(res.getItem());
    }

    @Test
    public void getItemWithoutKeyTest() {
        GetItemRequest request = new GetItemRequest();
        request.setTableName(tableName);
        GetItemResult res = getClient().getItem(request);
        Assert.assertNull(res.getItem());
    }

    @Test
    public void updateItemInTableTest() {
        AttributeValue hash = putItemInDb();
        Map<String, AttributeValueUpdate> attrToUp = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = new AttributeValueUpdate();
        update.setAction("PUT");
        update.setValue(hash);
        attrToUp.put("updated", update);
        Key key = new Key(hash);
        UpdateItemRequest request = new UpdateItemRequest(tableName, key, attrToUp);
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
        Key key = new Key(hash);
        UpdateItemRequest request = new UpdateItemRequest();
        request.setKey(key);
        request.setAttributeUpdates(attrToUp);
        UpdateItemResult res = getClient().updateItem(request);
        Assert.assertNull(res.getConsumedCapacityUnits());
    }

    @Test
    public void updateItemWithoutKeyTest() {
        AttributeValue hash = putItemInDb();
        Map<String, AttributeValueUpdate> attrToUp = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = new AttributeValueUpdate();
        update.setAction("PUT");
        update.setValue(hash);
        attrToUp.put("updated", update);
        Key key = new Key(hash);
        UpdateItemRequest request = new UpdateItemRequest();
        request.setTableName(tableName);
        request.setAttributeUpdates(attrToUp);
        UpdateItemResult res = getClient().updateItem(request);
        Assert.assertNull(res.getConsumedCapacityUnits());
    }

     @Test
    public void deleteItem() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        AttributeValue hash = new AttributeValue("ad"); //createStringAttribute();
        getClient().putItem(new PutItemRequest().withTableName(tableName).withItem(createGenericItem(hash)));
        DeleteItemRequest request = new DeleteItemRequest().withTableName(tableName).withKey(new Key(hash));
        DeleteItemResult result = getClient().deleteItem(request);
        Assert.assertNotNull(result.getConsumedCapacityUnits());
    }

    @Test
    public void deleteItemWithHashKeyAndRangeKey() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        schema.setRangeKeyElement(new KeySchemaElement().withAttributeName("range").withAttributeType(ScalarAttributeType.N));
        createTable(tableName, schema);
        AttributeValue hash = new AttributeValue("ad");
        AttributeValue range1 = new AttributeValue().withN("1");
        AttributeValue range2 = new AttributeValue().withN("2");
        getClient().putItem(new PutItemRequest().withTableName(tableName).withItem(createGenericItem(hash, range1)));
        getClient().putItem(new PutItemRequest().withTableName(tableName).withItem(createGenericItem(hash, range2)));
        Key key1 = new Key().withHashKeyElement(hash).withRangeKeyElement(range1);
        Key key2 = new Key().withHashKeyElement(hash).withRangeKeyElement(range2);
        DeleteItemRequest request1 = new DeleteItemRequest().withTableName(tableName).withKey(key1).withReturnValues(ReturnValue.ALL_OLD);
        DeleteItemRequest request2 = new DeleteItemRequest().withTableName(tableName).withKey(key2).withReturnValues(ReturnValue.ALL_OLD);
        DeleteItemResult result = getClient().deleteItem(request1);
        Assert.assertNotNull(result.getAttributes());
        result = getClient().deleteItem(request2);
        Assert.assertNotNull(result.getAttributes());
    }

    @Test
    public void deleteItemWithoutTableName() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        AttributeValue hash = createStringAttribute();
        getClient().putItem(new PutItemRequest().withTableName(tableName).withItem(createGenericItem(hash)));
        DeleteItemRequest request = new DeleteItemRequest().withKey(new Key(hash));
        DeleteItemResult result = getClient().deleteItem(request);
        Assert.assertNull(result.getConsumedCapacityUnits());
    }

    @Test
    public void deleteNonExistantItem() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        DeleteItemRequest request = new DeleteItemRequest().withTableName(tableName).withKey(new Key(createStringAttribute()));
        Assert.assertNull(getClient().deleteItem(request).getConsumedCapacityUnits());
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
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        AttributeValue hash = createStringAttribute();
        Map<String, AttributeValue> item = createGenericItem(hash);
        PutItemRequest req = new PutItemRequest().withTableName(tableName).withItem(item);
        getClient().putItem(req);
        return hash;
    }
}

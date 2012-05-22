package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.model.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;

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

    // TODO: need to test for different schemas

    @Test
    public void putItemWithHashKey() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(createGenericItem());
        PutItemResult res = client.putItem(request);
        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getConsumedCapacityUnits());
    }

    @Test
    public void putItemWithHashKeyOverwriteItem() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(createGenericItem());
        client.putItem(request); // put item beforehand
        PutItemResult res = client.putItem(request); // Add another
        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getConsumedCapacityUnits());
    }

    @Test
    public void putItemWithHashKeyWithoutTableName() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        PutItemRequest request = new PutItemRequest().withTableName(tableName);
        PutItemResult res = client.putItem(request);
        Assert.assertNull(res.getConsumedCapacityUnits());
    }

    @Test
    public void putItemWithHashKeyWithoutItem() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        PutItemRequest request = new PutItemRequest().withItem(createGenericItem());
        PutItemResult res = client.putItem(request);
        Assert.assertNull(res.getConsumedCapacityUnits());
    }

    // TODO: test out put item expected and return value
    @Test
    public void getItemTest() {
        AttributeValue hash = createItem(tableName);
        GetItemRequest request = new GetItemRequest().withTableName(tableName);
        request.setKey(new Key().withHashKeyElement(hash));
        GetItemResult res = client.getItem(request);
        Assert.assertNotNull(res.getItem());
        Assert.assertEquals(res.getItem().get("id"), hash);
    }

    @Test
    public void getItemWithoutTableNameTest() {
        GetItemRequest request = new GetItemRequest();
        request.setKey(new Key().withHashKeyElement(new AttributeValue().withNS("123")));
        GetItemResult res = client.getItem(request);
        Assert.assertNull(res.getItem());
    }

    @Test
    public void getItemWithoutKeyTest() {
        GetItemRequest request = new GetItemRequest();
        request.setTableName(tableName);
        GetItemResult res = client.getItem(request);
        Assert.assertNull(res.getItem());
    }

    @Test
    public void updateItemInTableTest() {
        AttributeValue hash = putItemInDb();
        Map<String, AttributeValueUpdate> attrToUp = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = new AttributeValueUpdate();
        update.setAction("PUT");
        update.setValue(hash);
        attrToUp.put("updated",update);
        Key key = new Key(hash);
        UpdateItemRequest request = new UpdateItemRequest(tableName, key, attrToUp);
        UpdateItemResult res = client.updateItem(request);
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
        attrToUp.put("updated",update);
        Key key = new Key(hash);
        UpdateItemRequest request = new UpdateItemRequest();
        request.setKey(key);
        request.setAttributeUpdates(attrToUp);
        UpdateItemResult res = client.updateItem(request);
        Assert.assertNull(res.getConsumedCapacityUnits());
    }

    @Test
    public void updateItemWithoutKeyTest() {
        AttributeValue hash = putItemInDb();
        Map<String, AttributeValueUpdate> attrToUp = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = new AttributeValueUpdate();
        update.setAction("PUT");
        update.setValue(hash);
        attrToUp.put("updated",update);
        Key key = new Key(hash);
        UpdateItemRequest request = new UpdateItemRequest();
        request.setTableName(tableName);
        request.setAttributeUpdates(attrToUp);
        UpdateItemResult res = client.updateItem(request);
        Assert.assertNull(res.getConsumedCapacityUnits());
    }

    @Test
    public void deleteItem() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        AttributeValue hash = new AttributeValue("ad"); //createStringAttribute();
        client.putItem(new PutItemRequest().withTableName(tableName).withItem(createGenericItem(hash)));
        DeleteItemRequest request = new DeleteItemRequest().withTableName(tableName).withKey(new Key(hash));
        DeleteItemResult result = client.deleteItem(request);
        Assert.assertNotNull(result.getConsumedCapacityUnits());
    }

    @Test
    public void deleteItemWithoutTableName() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        AttributeValue hash = createStringAttribute();
        client.putItem(new PutItemRequest().withTableName(tableName).withItem(createGenericItem(hash)));
        DeleteItemRequest request = new DeleteItemRequest().withKey(new Key(hash));
        DeleteItemResult result = client.deleteItem(request);
        Assert.assertNull(result.getConsumedCapacityUnits());
    }

    @Test
    public void deleteNonExistantItem() {
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        DeleteItemRequest request = new DeleteItemRequest().withTableName(tableName).withKey(new Key(createStringAttribute()));
        Assert.assertNull(client.deleteItem(request).getConsumedCapacityUnits());
    }

    // TODO: test out delete item expected and return value

/*
	@Test
	public void batchWriteItemInTableTest() {
		BatchWriteItemResult result = client.batchWriteItem(generateWriteBatchRequest());
		Assert.assertNotNull(result);
	}

	@Test
	public void batchGetItemInTableTest() {
		BatchGetItemResult result = client.batchGetItem(generateGetBatchRequest());
		Assert.assertNotNull(result);
	}

	@Test
	public void batchGetItemInTableWithoutKeyTest() {
		BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest();
		Map<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();
		requestItems.put(testTableName, null);
		batchGetItemRequest.withRequestItems(requestItems);
		Assert.assertNull(client.batchGetItem(batchGetItemRequest).getResponses());
	}

	@Test
	public void batchGetItemInTableWithoutNameTest() {
		BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest();
		Map<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();
		Key table1key1 = new Key().withHashKeyElement(new AttributeValue().withS("123"));
		requestItems.put(null, new KeysAndAttributes().withKeys(table1key1));
		batchGetItemRequest.withRequestItems(requestItems);
		Assert.assertNull(client.batchGetItem(batchGetItemRequest).getResponses());
	}

	@Test
	public void batchGetItemInTableWithoutRequestItemsTest() {
		Assert.assertNull(client.batchGetItem(new BatchGetItemRequest()).getResponses());
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
        client.putItem(req);
        return hash;
    }
}

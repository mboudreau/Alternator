package com.michelboudreau.testv2;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.michelboudreau.alternatorv2.AlternatorDBInProcessClientV2;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext.xml"})
public class AlternatorUpdateTest extends AlternatorTest {
	protected final String PERSISTENCE_PATH = "./AlternatorSaveRestoreTest.db";

    private String tableName;

    @Before
    public void setUp() throws Exception {
        tableName = createTableName();
    }

    @After
    public void tearDown() throws Exception {
        deleteAllTables();
    }

    /**
     * Override the getClient method from the AlternatorTest base class to use an in-process client.
     * That client exposes .save and .restore method wrappers for the AlternatorDBHandler.
     *
     * @return an instance of AlternatorDBInProcessClient as an AmazonDynamoDB instance.
     */
    @Override
    protected AmazonDynamoDB getClient() {
        if (inProcessClient == null) {
            inProcessClient = new AlternatorDBInProcessClientV2();
        }
        return inProcessClient;
    }

    protected AlternatorDBInProcessClientV2 createNewInProcessClient() {
        inProcessClient = new AlternatorDBInProcessClientV2();
        return inProcessClient;
    }

    @Test
    public void queryUpdate() {
        // Setup table with items
        createGenericTable(tableName);

        Map<String, AttributeValueUpdate> dynValues = new HashMap<String, AttributeValueUpdate> ();
        dynValues.put("count", new AttributeValueUpdate(new AttributeValue().withN("100"), AttributeAction.ADD));

        AttributeValue hashKey = new AttributeValue().withS("1");
        UpdateItemRequest update = new UpdateItemRequest(tableName, createItemKey("id", hashKey), dynValues);

        getClient().updateItem(update);

        inProcessClient.save(PERSISTENCE_PATH);
        createNewInProcessClient().restore(PERSISTENCE_PATH);

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;
        QueryResult result = getClient().query(request);
        Assert.assertNotNull(result.getItems());
        Assert.assertNotSame(result.getItems().size(), 0);

        Map<String, AttributeValue> row = result.getItems().get(0);
        Assert.assertEquals(row.get("id"), hashKey);
        Assert.assertEquals(row.get("count").getN(), "100");
    }

    @Test
    public void accumulativeUpdate() {
        // Setup table with items
        createGenericTable(tableName);

        AttributeValue hashKey = new AttributeValue().withS("1");

        Map<String, AttributeValueUpdate> dynValues = new HashMap<String, AttributeValueUpdate> ();
        dynValues.put("count", new AttributeValueUpdate(new AttributeValue().withNS("100"), AttributeAction.ADD));

        UpdateItemRequest update = new UpdateItemRequest(tableName, createItemKey("id", hashKey), dynValues);

        getClient().updateItem(update);

        //second update
        dynValues = new HashMap<String, AttributeValueUpdate> ();
        dynValues.put("count", new AttributeValueUpdate(new AttributeValue().withNS("102"), AttributeAction.ADD));
        update = new UpdateItemRequest(tableName, createItemKey("id", hashKey), dynValues);
        getClient().updateItem(update);

        inProcessClient.save(PERSISTENCE_PATH);
        createNewInProcessClient().restore(PERSISTENCE_PATH);

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;
        QueryResult result = getClient().query(request);
        Assert.assertNotNull(result.getItems());
        Assert.assertNotSame(result.getItems().size(), 0);
        Map<String, AttributeValue> row = result.getItems().get(0);

        Assert.assertEquals(row.get("id"), hashKey);
        Assert.assertArrayEquals(row.get("count").getNS().toArray(), new String[]{"100", "102"});
    }

    @Test
    public void conditionalUpdateOnHit(){
    	// Setup table with items
        createGenericTable(tableName);

        AttributeValue hashKey = new AttributeValue().withS("1");

        Map<String, AttributeValueUpdate> oldValues = new HashMap<String, AttributeValueUpdate> ();
        oldValues.put("count", new AttributeValueUpdate(new AttributeValue().withN("100"), AttributeAction.PUT));

        UpdateItemRequest update = new UpdateItemRequest(tableName, createItemKey("id", hashKey), oldValues);

        getClient().updateItem(update);

        //conditional update
        HashMap<String, AttributeValueUpdate> newValues = new HashMap<String, AttributeValueUpdate> ();
        newValues.put("count", new AttributeValueUpdate(new AttributeValue().withN("102"), AttributeAction.PUT));

        HashMap<String, ExpectedAttributeValue> expectedValues = new HashMap<String, ExpectedAttributeValue> ();
        expectedValues.put("count", new ExpectedAttributeValue().withValue(new AttributeValue().withN("100")));

        update = new UpdateItemRequest().withTableName(tableName).withKey(createItemKey("id", hashKey)).withAttributeUpdates(newValues).withExpected(expectedValues);
        getClient().updateItem(update);

        inProcessClient.save(PERSISTENCE_PATH);
        createNewInProcessClient().restore(PERSISTENCE_PATH);

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;
        QueryResult result = getClient().query(request);
        Assert.assertNotNull(result.getItems());
        Assert.assertNotSame(result.getItems().size(), 0);

        Map<String, AttributeValue> row = result.getItems().get(0);
        Assert.assertEquals(row.get("id"), hashKey);
        Assert.assertEquals(row.get("count").getN(), "102");
    }

    @Test
    public void conditionalAddNewFieldUpdateOnHit(){
    	// Setup table with items
        createGenericTable(tableName);

        AttributeValue hashKey = new AttributeValue().withS("1");

        Map<String, AttributeValueUpdate> oldValues = new HashMap<String, AttributeValueUpdate> ();
        oldValues.put("count", new AttributeValueUpdate(new AttributeValue().withN("100"), AttributeAction.PUT));

        UpdateItemRequest update = new UpdateItemRequest(tableName, createItemKey("id", hashKey), oldValues);

        getClient().updateItem(update);

        //conditional update
        HashMap<String, AttributeValueUpdate> newValues = new HashMap<String, AttributeValueUpdate> ();
        newValues.put("count", new AttributeValueUpdate(new AttributeValue().withN("102"), AttributeAction.PUT));
        newValues.put("ids", new AttributeValueUpdate(new AttributeValue().withS("[er, er]"), AttributeAction.ADD));

        HashMap<String, ExpectedAttributeValue> expectedValues = new HashMap<String, ExpectedAttributeValue> ();
        expectedValues.put("count", new ExpectedAttributeValue().withValue(new AttributeValue().withN("100")));

        update = new UpdateItemRequest().withTableName(tableName).withKey(createItemKey("id", hashKey)).withAttributeUpdates(newValues).withExpected(expectedValues);
        getClient().updateItem(update);

        inProcessClient.save(PERSISTENCE_PATH);
        createNewInProcessClient().restore(PERSISTENCE_PATH);

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;
        QueryResult result = getClient().query(request);
        Assert.assertNotNull(result.getItems());
        Assert.assertNotSame(result.getItems().size(), 0);

        Map<String, AttributeValue> row = result.getItems().get(0);
        Assert.assertEquals(row.get("id"), hashKey);
        Assert.assertEquals(row.get("count").getN(), "102");
        Assert.assertEquals(row.get("ids").getS(), "[er, er]");
    }

    @Test
    public void conditionalPutNewFieldUpdateOnHit(){
    	// Setup table with items
        createGenericTable(tableName);

        AttributeValue hashKey = new AttributeValue().withS("1");

        Map<String, AttributeValueUpdate> oldValues = new HashMap<String, AttributeValueUpdate> ();
        oldValues.put("count", new AttributeValueUpdate(new AttributeValue().withN("100"), AttributeAction.PUT));

        UpdateItemRequest update = new UpdateItemRequest(tableName, createItemKey("id", hashKey), oldValues);

        getClient().updateItem(update);

        //conditional update
        HashMap<String, AttributeValueUpdate> newValues = new HashMap<String, AttributeValueUpdate> ();
        newValues.put("count", new AttributeValueUpdate(new AttributeValue().withN("102"), AttributeAction.PUT));
        newValues.put("ids", new AttributeValueUpdate(new AttributeValue().withS("[er, er]"), AttributeAction.PUT));

        HashMap<String, ExpectedAttributeValue> expectedValues = new HashMap<String, ExpectedAttributeValue> ();
        expectedValues.put("count", new ExpectedAttributeValue().withValue(new AttributeValue().withN("100")));

        update = new UpdateItemRequest().withTableName(tableName).withKey(createItemKey("id", hashKey)).withAttributeUpdates(newValues).withExpected(expectedValues);
        getClient().updateItem(update);

        inProcessClient.save(PERSISTENCE_PATH);
        createNewInProcessClient().restore(PERSISTENCE_PATH);

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;
        QueryResult result = getClient().query(request);
        Assert.assertNotNull(result.getItems());
        Assert.assertNotSame(result.getItems().size(), 0);

        Map<String, AttributeValue> row = result.getItems().get(0);
        Assert.assertEquals(row.get("id"), hashKey);
        Assert.assertEquals(row.get("count").getN(), "102");
        Assert.assertEquals(row.get("ids").getS(), "[er, er]");
    }

    @Test
    public void conditionalUpdateOnMissing(){
    	// Setup table with items
        createGenericTable(tableName);

        AttributeValue hashKey = new AttributeValue().withS("1");

        Map<String, AttributeValueUpdate> oldValues = new HashMap<String, AttributeValueUpdate> ();
        oldValues.put("count", new AttributeValueUpdate(new AttributeValue().withN("100"), AttributeAction.PUT));

        UpdateItemRequest update = new UpdateItemRequest(tableName, createItemKey("id", hashKey), oldValues);

        getClient().updateItem(update);

        //conditional update
        HashMap<String, AttributeValueUpdate> newValues = new HashMap<String, AttributeValueUpdate> ();
        newValues.put("count", new AttributeValueUpdate(new AttributeValue().withN("102"), AttributeAction.PUT));

        HashMap<String, ExpectedAttributeValue> expectedValues = new HashMap<String, ExpectedAttributeValue> ();
        expectedValues.put("count", new ExpectedAttributeValue().withValue(new AttributeValue().withN("10")));

        update = new UpdateItemRequest().withTableName(tableName).withKey(createItemKey("id", hashKey)).withAttributeUpdates(newValues).withExpected(expectedValues);

        try{
	        getClient().updateItem(update);

	        inProcessClient.save(PERSISTENCE_PATH);
	        createNewInProcessClient().restore(PERSISTENCE_PATH);
	        Assert.fail("expecting conditional check exception");
        }catch (ConditionalCheckFailedException e) {
			//expect this exception
		}
    }

    @Test
    public void conditionalDeleteOldFieldUpdateOnHit(){
    	// Setup table with items
        createGenericTable(tableName);

        AttributeValue hashKey = new AttributeValue().withS("1");

        Map<String, AttributeValueUpdate> oldValues = new HashMap<String, AttributeValueUpdate> ();
        oldValues.put("count", new AttributeValueUpdate(new AttributeValue().withN("100"), AttributeAction.PUT));
        oldValues.put("ids", new AttributeValueUpdate(new AttributeValue().withS("[er, er]"), AttributeAction.ADD));

        UpdateItemRequest update = new UpdateItemRequest(tableName, createItemKey("id", hashKey), oldValues);

        getClient().updateItem(update);

        //conditional update
        HashMap<String, AttributeValueUpdate> newValues = new HashMap<String, AttributeValueUpdate> ();
        newValues.put("count", new AttributeValueUpdate(new AttributeValue().withN("102"), AttributeAction.PUT));
        newValues.put("ids", new AttributeValueUpdate(new AttributeValue().withS("[er, er]"), AttributeAction.DELETE));

        HashMap<String, ExpectedAttributeValue> expectedValues = new HashMap<String, ExpectedAttributeValue> ();
        expectedValues.put("count", new ExpectedAttributeValue().withValue(new AttributeValue().withN("100")));

        update = new UpdateItemRequest().withTableName(tableName).withKey(createItemKey("id", hashKey)).withAttributeUpdates(newValues).withExpected(expectedValues);
        getClient().updateItem(update);

        inProcessClient.save(PERSISTENCE_PATH);
        createNewInProcessClient().restore(PERSISTENCE_PATH);

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;
        QueryResult result = getClient().query(request);
        Assert.assertNotNull(result.getItems());
        Assert.assertNotSame(result.getItems().size(), 0);

        Map<String, AttributeValue> row = result.getItems().get(0);
        Assert.assertEquals(row.get("id"), hashKey);
        Assert.assertEquals(row.get("count").getN(), "102");
        Assert.assertNull(row.get("ids"));
    }
}

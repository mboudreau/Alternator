package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ScalarAttributeType;
import com.michelboudreau.alternator.AlternatorDBInProcessClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext.xml"})
public class AlternatorSaveRestoreTest extends AlternatorTest {

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
            inProcessClient = new AlternatorDBInProcessClient();
        }
        return inProcessClient;
    }

    /**
     * Reconstruct a new instance of AlternatorDBInProcessClient to force empty in-memory data structures
     * for our list of Tables and each Table's list of ItemRangeGroups.
     * 
     * @return a new instance of AlternatorDBInProcessClient.
     */
    protected AlternatorDBInProcessClient createNewInProcessClient() {
        inProcessClient = new AlternatorDBInProcessClient();
        return inProcessClient;
    }

    @Test
    public void queryWithHashKey() {
        // Setup table with items
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        AttributeValue hashKey = createStringAttribute();
        getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey)).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));

        inProcessClient.save(PERSISTENCE_PATH);
        createNewInProcessClient().restore(PERSISTENCE_PATH);

        QueryRequest request = new QueryRequest(tableName, hashKey);
        QueryResult result = getClient().query(request);
        Assert.assertNotNull(result.getItems());
        Assert.assertNotSame(result.getItems().size(), 0);
        Assert.assertEquals(result.getItems().get(0).get("id"), hashKey);
    }

    @Test
    public void queryWithHashKeyAndRangeKeyConditionEQTest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        inProcessClient.save(PERSISTENCE_PATH);
        createNewInProcessClient().restore(PERSISTENCE_PATH);

        QueryRequest request = new QueryRequest().withTableName(tableName).withHashKeyValue(hashKey);

        Condition rangeKeyCondition = new Condition();
        List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
        attributeValueList.add(createStringAttribute("Range2"));
        rangeKeyCondition.setAttributeValueList(attributeValueList);
        rangeKeyCondition.setComparisonOperator(ComparisonOperator.EQ);
        request.setRangeKeyCondition(rangeKeyCondition);
        QueryResult result = getClient().query(request);
        Assert.assertNotNull("Null result.", result);
        Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return one item.", 1, result.getItems().size());
        for (Map<String, AttributeValue> item : result.getItems()) {
            Assert.assertEquals(item.get("range").getS(), "Range2");
        }
    }

    private AttributeValue setupTableWithSeveralItems() {
        KeySchema schema = new KeySchema()
                .withHashKeyElement(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S))
                .withRangeKeyElement(new KeySchemaElement().withAttributeName("range").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);

        AttributeValue hashKey1 = createStringAttribute();
        AttributeValue hashKey2 = createStringAttribute();

        getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));

        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey1, createStringAttribute("Range4"), "attr1", "value14", "attr2", "value24")).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey1, createStringAttribute("Range3"), "attr1", "value13", "attr2", "value23")).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey1, createStringAttribute("Range2"), "attr1", "value12", "attr2", "value22")).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey1, createStringAttribute("Range1"), "attr1", "value11", "attr2", "value21")).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey1, createStringAttribute("AnotherRange1"), "attr1", "value19", "attr2", "value29")).withTableName(tableName));

        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey2, createStringAttribute("AnotherRange1"), "attr1", "value19", "attr2", "value29")).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey2, createStringAttribute("Range1"), "attr1", "value11", "attr2", "value21")).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey2, createStringAttribute("Range2"), "attr1", "value12", "attr2", "value22")).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey2, createStringAttribute("Range3"), "attr1", "value13", "attr2", "value23")).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey2, createStringAttribute("Range4"), "attr1", "value14", "attr2", "value24")).withTableName(tableName));

        getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));

        return hashKey1;
    }
}

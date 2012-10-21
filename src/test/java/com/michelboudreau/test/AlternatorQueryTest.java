package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.model.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext.xml"})
public class AlternatorQueryTest extends AlternatorTest {

	private String tableName;

	@Before
	public void setUp() throws Exception {
		tableName = createTableName();
	}

	@After
	public void tearDown() throws Exception {
		deleteAllTables();
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

		QueryRequest request = new QueryRequest(tableName, hashKey);
		QueryResult result = getClient().query(request);
		Assert.assertNotNull(result.getItems());
		Assert.assertNotSame(result.getItems().size(), 0);
		Assert.assertEquals(result.getItems().get(0).get("id"), hashKey);
	}

    //I do not think this is a good design since
    @Test
    public void queryWithHashKeyNotExist() {
        // Setup table with items
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        AttributeValue hashKey = createStringAttribute();
        getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));

        QueryRequest request = new QueryRequest(tableName, hashKey);
        Assert.assertNotNull(request);
        QueryResult result = getClient().query(request);
        Assert.assertNotNull(result.getItems());     //result should be null but unfortunately it not.
        Assert.assertSame(result.getItems().size(), 0);
    }

    private AttributeValue setupTableWithSeveralItems() {
		KeySchema schema = new KeySchema()
                .withHashKeyElement(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S))
                .withRangeKeyElement(new KeySchemaElement().withAttributeName("range").withAttributeType(ScalarAttributeType.S))
                ;
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

    @Test
	public void queryWithHashKeyAndAttributesToGetTest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request = new QueryRequest().withTableName(tableName).withHashKeyValue(hashKey);

		List<String> attrToGet = new ArrayList<String>();
		attrToGet.add("range");
		attrToGet.add("attr1");
		attrToGet.add("bogusAttr");
		request.setAttributesToGet(attrToGet);
		QueryResult result = getClient().query(request);
		Assert.assertNotNull(result);
		Assert.assertNotNull(result.getItems());
		Assert.assertNotNull(result.getItems().get(0));
		Assert.assertFalse("First item should not contain 'id'.", result.getItems().get(0).containsKey("id"));
		Assert.assertTrue("First item missing 'range'.", result.getItems().get(0).containsKey("range"));
		Assert.assertTrue("First item missing 'attr1'.", result.getItems().get(0).containsKey("attr1"));
		Assert.assertFalse("First item should not contain 'bogusAttr'.", result.getItems().get(0).containsKey("bogusAttr"));
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionEQTest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request = new QueryRequest().withTableName(tableName).withHashKeyValue(hashKey);

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
//		attributeValueList.add(new AttributeValue().withN("1"));
		attributeValueList.add(createStringAttribute("Range2"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.EQ);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = getClient().query(request);
		Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return a one item.", 1, result.getItems().size());
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertEquals(item.get("range").getS(), "Range2");
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionLTTest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request = new QueryRequest().withTableName(tableName).withHashKeyValue(hashKey);

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
//		attributeValueList.add(new AttributeValue().withN("1"));
		attributeValueList.add(createStringAttribute("Range2"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.LT);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = getClient().query(request);
		Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return a two items.", 2, result.getItems().size());
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(item.get("range").getS().compareTo("Range2") < 0);
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionLETest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request = new QueryRequest().withTableName(tableName).withHashKeyValue(hashKey);

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
//		attributeValueList.add(new AttributeValue().withN("1"));
		attributeValueList.add(createStringAttribute("Range2"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.LE);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = getClient().query(request);
		Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return three items.", 3, result.getItems().size());
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(item.get("range").getS().compareTo("Range2") <= 0);
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionGTTest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request = new QueryRequest().withTableName(tableName).withHashKeyValue(hashKey);

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
//		attributeValueList.add(new AttributeValue().withN("1"));
		attributeValueList.add(createStringAttribute("Range2"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.GT);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = getClient().query(request);
		Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return two items.", 2, result.getItems().size());
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(item.get("range").getS().compareTo("Range2") > 0);
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionGETest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request = new QueryRequest().withTableName(tableName).withHashKeyValue(hashKey);

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
//		attributeValueList.add(new AttributeValue().withN("1"));
		attributeValueList.add(createStringAttribute("Range2"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.GE);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = getClient().query(request);
		Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return three items.", 3, result.getItems().size());
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(item.get("range").getS().compareTo("Range2") >= 0);
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionINTest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request = new QueryRequest().withTableName(tableName).withHashKeyValue(hashKey);

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withS("Range2"));
		attributeValueList.add(new AttributeValue().withS("Range4"));
		attributeValueList.add(new AttributeValue().withS("Range0"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.IN);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = getClient().query(request);
		Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return two items.", 2, result.getItems().size());
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(
                    (item.get("range").getS().compareTo("Range2") == 0)
                    ||
                    (item.get("range").getS().compareTo("Range4") == 0)
                    );
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionBETWEENTest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request = new QueryRequest().withTableName(tableName).withHashKeyValue(hashKey);

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withS("Range2"));
		attributeValueList.add(new AttributeValue().withS("Range3"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.BETWEEN);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = getClient().query(request);
		Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return two items.", 2, result.getItems().size());
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(
                    (item.get("range").getS().compareTo("Range2") == 0)
                    ||
                    (item.get("range").getS().compareTo("Range3") == 0)
                    );
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionBEGINSWITHTest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request = new QueryRequest().withTableName(tableName).withHashKeyValue(hashKey);

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withS("Range"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.BEGINS_WITH);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = getClient().query(request);
		Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return four items.", 4, result.getItems().size());
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(
                    item.get("range").getS().startsWith("Range")
                    );
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionCONTAINSTest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request = new QueryRequest().withTableName(tableName).withHashKeyValue(hashKey);

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withS("ange1"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.CONTAINS);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = getClient().query(request);
		Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return two items.", 2, result.getItems().size());
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(
                    item.get("range").getS().contains("ange1")
                    );
		}
	}

    /*
	@Test
	public void queryWithHashKeyAndLimitTest() {
		QueryRequest request = getBasicReq();
		request.setLimit(1);
		QueryResult result = getClient().query(request);
		Assert.assertNotNull(result);
		Assert.assertEquals(result.getCount().intValue(), 1);
	}

	@Test
	public void queryWithoutTableNameTest() {
		QueryRequest request = getBasicReq();
		request.setTableName(null);
		QueryResult result = getClient().query(request);
		Assert.assertNull(result.getItems());
	}*/
}

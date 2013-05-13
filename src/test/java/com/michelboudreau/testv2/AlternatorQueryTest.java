package com.michelboudreau.testv2;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
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
        createGenericTable(tableName);

        AttributeValue hashKey = createStringAttribute();
		getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));
		getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey)).withTableName(tableName));
		getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));
		getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;
		QueryResult result = getClient().query(request);

        Assert.assertNotNull(result.getItems());
		Assert.assertNotSame(result.getItems().size(), 0);
		Assert.assertEquals(result.getItems().get(0).get("id"), hashKey);
	}

    //I do not think this is a good design since
    @Test
    public void queryWithHashKeyNotExist() {
        // Setup table with items
        createGenericTable(tableName);

        AttributeValue hashKey = createStringAttribute();
        getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem()).withTableName(tableName));

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;
        Assert.assertNotNull(request);

        QueryResult result = getClient().query(request);

        Assert.assertNotNull(result.getItems());     //result should be null but unfortunately it not.
        Assert.assertSame(result.getItems().size(), 0);
    }

    private AttributeValue setupTableWithSeveralItems() {
        createGenericHashRangeTable(tableName);

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

    private AttributeValue setupNumericRangeTableWithSeveralItems() {
        AttributeDefinition hashAttr = createStringAttributeDefinition("id");
        AttributeDefinition rangeAttr = createNumberAttributeDefinition("range");
        createTable(tableName, hashAttr, rangeAttr);

		AttributeValue hashKey1 = createStringAttribute();
		AttributeValue hashKey2 = createStringAttribute();

		getClient().putItem(new PutItemRequest().withItem(createGenericItem(createStringAttribute(), createNumberAttribute())).withTableName(tableName));

        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey1, createNumberAttribute(4), "attr1", "value14", "attr2", "value24")).withTableName(tableName));
		getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey1, createNumberAttribute(3), "attr1", "value13", "attr2", "value23")).withTableName(tableName));
		getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey1, createNumberAttribute(2), "attr1", "value12", "attr2", "value22")).withTableName(tableName));
		getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey1, createNumberAttribute(1), "attr1", "value11", "attr2", "value21")).withTableName(tableName));
		getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey1, createNumberAttribute(11), "attr1", "value11", "attr2", "value21")).withTableName(tableName));
		getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey1, createNumberAttribute(51), "attr1", "value19", "attr2", "value29")).withTableName(tableName));

		getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey2, createNumberAttribute(51), "attr1", "value19", "attr2", "value29")).withTableName(tableName));
		getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey2, createNumberAttribute(11), "attr1", "value19", "attr2", "value29")).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey2, createNumberAttribute(1), "attr1", "value11", "attr2", "value21")).withTableName(tableName));
		getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey2, createNumberAttribute(2), "attr1", "value12", "attr2", "value22")).withTableName(tableName));
		getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey2, createNumberAttribute(3), "attr1", "value13", "attr2", "value23")).withTableName(tableName));
		getClient().putItem(new PutItemRequest().withItem(createGenericItem(hashKey2, createNumberAttribute(4), "attr1", "value14", "attr2", "value24")).withTableName(tableName));

        getClient().putItem(new PutItemRequest().withItem(createGenericItem(createStringAttribute(), createNumberAttribute())).withTableName(tableName));
        getClient().putItem(new PutItemRequest().withItem(createGenericItem(createStringAttribute(), createNumberAttribute())).withTableName(tableName));

        return hashKey1;
    }

    @Test
	public void queryWithHashKeyAndAttributesToGetTest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

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

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
//		attributeValueList.add(new AttributeValue().withN("1"));
		attributeValueList.add(createStringAttribute("Range2"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.EQ);
		request.getKeyConditions().put("range", rangeKeyCondition);

        QueryResult result = getClient().query(request);

        Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return one item.", 1, result.getItems().size());

        for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertEquals(item.get("range").getS(), "Range2");
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionLTTest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
//		attributeValueList.add(new AttributeValue().withN("1"));
		attributeValueList.add(createStringAttribute("Range2"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.LT);
		request.getKeyConditions().put("range", rangeKeyCondition);

        QueryResult result = getClient().query(request);

        Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return two items.", 2, result.getItems().size());

        for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(item.get("range").getS().compareTo("Range2") < 0);
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionLETest() {
        AttributeValue hashKey = setupTableWithSeveralItems();

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
//		attributeValueList.add(new AttributeValue().withN("1"));
		attributeValueList.add(createStringAttribute("Range2"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.LE);
		request.getKeyConditions().put("range", rangeKeyCondition);

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

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
//		attributeValueList.add(new AttributeValue().withN("1"));
		attributeValueList.add(createStringAttribute("Range2"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.GT);
		request.getKeyConditions().put("range", rangeKeyCondition);

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

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
//		attributeValueList.add(new AttributeValue().withN("1"));
		attributeValueList.add(createStringAttribute("Range2"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.GE);
		request.getKeyConditions().put("range", rangeKeyCondition);

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

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withS("Range2"));
		attributeValueList.add(new AttributeValue().withS("Range4"));
		attributeValueList.add(new AttributeValue().withS("Range0"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.IN);
		request.getKeyConditions().put("range", rangeKeyCondition);

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

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withS("Range2"));
		attributeValueList.add(new AttributeValue().withS("Range3"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.BETWEEN);
		request.getKeyConditions().put("range", rangeKeyCondition);

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

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withS("Range"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.BEGINS_WITH);
		request.getKeyConditions().put("range", rangeKeyCondition);

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

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withS("ange1"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.CONTAINS);
		request.getKeyConditions().put("range", rangeKeyCondition);

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

	@Test
	public void queryWithHashKeyAndNumericRangeKeyConditionEQTest() {
        AttributeValue hashKey = setupNumericRangeTableWithSeveralItems();

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(createNumberAttribute(2));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.EQ);
		request.getKeyConditions().put("range", rangeKeyCondition);

        QueryResult result = getClient().query(request);

        Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return one item.", 1, result.getItems().size());

        for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertEquals(item.get("range").getN(), "2");
		}
	}

	@Test
	public void queryWithHashKeyAndNumericRangeKeyConditionLTTest() {
        AttributeValue hashKey = setupNumericRangeTableWithSeveralItems();

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(createNumberAttribute(2));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.LT);
		request.getKeyConditions().put("range", rangeKeyCondition);

        QueryResult result = getClient().query(request);

        Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        // NOTE: LT is currently a string comparison, so "11" is < "2".
        Assert.assertEquals("Should return two items.", 2, result.getItems().size());
	}

	@Test
	public void queryWithHashKeyAndNumericRangeKeyConditionLETest() {
        AttributeValue hashKey = setupNumericRangeTableWithSeveralItems();

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(createNumberAttribute(2));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.LE);
		request.getKeyConditions().put("range", rangeKeyCondition);

        QueryResult result = getClient().query(request);

        Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        // NOTE: LE is currently a string comparison, so "11" is <= "2".
        Assert.assertEquals("Should return three items.", 3, result.getItems().size());
	}

	@Test
	public void queryWithHashKeyAndNumericRangeKeyConditionGTTest() {
        AttributeValue hashKey = setupNumericRangeTableWithSeveralItems();

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(createNumberAttribute(2));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.GT);
		request.getKeyConditions().put("range", rangeKeyCondition);

        QueryResult result = getClient().query(request);

        Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        // NOTE: GT is currently a string comparison, so "11" is NOT > "2".
        Assert.assertEquals("Should return three items.", 3, result.getItems().size());
	}

	@Test
	public void queryWithHashKeyAndNumericRangeKeyConditionGETest() {
        AttributeValue hashKey = setupNumericRangeTableWithSeveralItems();

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(createNumberAttribute(2));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.GE);
		request.getKeyConditions().put("range", rangeKeyCondition);

        QueryResult result = getClient().query(request);

        Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        // NOTE: GE is currently a string comparison, so "11" is NOT > "2".
        Assert.assertEquals("Should return four items.", 4, result.getItems().size());
	}

	@Test
	public void queryWithHashKeyAndNumericRangeKeyConditionINTest() {
        AttributeValue hashKey = setupNumericRangeTableWithSeveralItems();

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(createNumberAttribute(2));
		attributeValueList.add(createNumberAttribute(4));
		attributeValueList.add(createNumberAttribute(0));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.IN);
		request.getKeyConditions().put("range", rangeKeyCondition);

        QueryResult result = getClient().query(request);

        Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return two items.", 2, result.getItems().size());
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(
                    (item.get("range").getN().compareTo("2") == 0)
                    ||
                    (item.get("range").getN().compareTo("4") == 0)
                    );
		}
	}

	@Test
	public void queryWithHashKeyAndNumericRangeKeyConditionBETWEENTest() {
        AttributeValue hashKey = setupNumericRangeTableWithSeveralItems();

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(createNumberAttribute(2));
		attributeValueList.add(createNumberAttribute(3));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.BETWEEN);
		request.getKeyConditions().put("range", rangeKeyCondition);

        QueryResult result = getClient().query(request);

        Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        Assert.assertEquals("Should return two items.", 2, result.getItems().size());
	}

	@Test
	public void queryWithHashKeyAndNumericRangeKeyConditionBEGINSWITHTest() {
        AttributeValue hashKey = setupNumericRangeTableWithSeveralItems();

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(createNumberAttribute(1));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.BEGINS_WITH);
		request.getKeyConditions().put("range", rangeKeyCondition);

        QueryResult result = getClient().query(request);

        Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        // NOTE: BEGINS_WITH is currently a string comparison, so "11" begins with "1".
        Assert.assertEquals("Should return two items.", 2, result.getItems().size());
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(
                    item.get("range").getN().startsWith("1")
                    );
		}
	}

	@Test
	public void queryWithHashKeyAndNumericRangeKeyConditionCONTAINSTest() {
        AttributeValue hashKey = setupNumericRangeTableWithSeveralItems();

        QueryRequest request =
            new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(createHashKeyCondition("id", hashKey))
                ;

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(createNumberAttribute(1));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.CONTAINS);
		request.getKeyConditions().put("range", rangeKeyCondition);

        QueryResult result = getClient().query(request);

        Assert.assertNotNull("Null result.", result);
		Assert.assertNotNull("No items returned.", result.getItems());
        // NOTE: CONTAINS is currently a string comparison, so "1", "11", "51" all contain "1".
        Assert.assertEquals("Should return three items.", 3, result.getItems().size());
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(
                    item.get("range").getN().contains("1")
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

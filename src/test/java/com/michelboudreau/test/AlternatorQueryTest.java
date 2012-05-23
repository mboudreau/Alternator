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
		/*KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
		createTable(tableName, schema);
		AttributeValue hashKey = createStringAttribute();
		createGenericItem();
		createGenericItem(hashKey);
		createGenericItem();
		createGenericItem();

		QueryRequest request = new QueryRequest(tableName, hashKey);
		QueryResult result = client.query(request);
		Assert.assertNotNull(result.getItems());
		Assert.assertNotSame(result.getItems().size(), 0);
		Assert.assertEquals(result.getItems().get(0).get("id"), hashKey);*/
	}
/*
	@Test
	public void queryWithHashKeyAndAttributesToGetTest() {
		QueryRequest request = getBasicReq();
		request.setHashKeyValue(getHashKey());
		List<String> attrToGet = new ArrayList<String>();
		attrToGet.add("date");
		attrToGet.add("testfield");
		request.setAttributesToGet(attrToGet);
		QueryResult result = client.query(request);
		Assert.assertNotNull(result);
		Assert.assertNotNull(result.getItems());
		Assert.assertNotNull(result.getItems().get(0));
		Assert.assertFalse(result.getItems().get(0).containsKey("id"));
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionEQTest() {
		QueryRequest request = getBasicReq();
		request.setHashKeyValue(getHashKey());
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("1"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.EQ);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = client.query(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertEquals(item.get("date").getN(), "1");
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionGETest() {
		QueryRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("1"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.GE);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = client.query(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue((new Integer(item.get("date").getN())) >= 1);
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionGTTest() {
		QueryRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("1"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.GT);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = client.query(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue((new Integer(item.get("date").getN())) > 1);
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionLETest() {
		QueryRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("10"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.LE);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = client.query(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue((new Integer(item.get("date").getN())) <= 1);
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionLTTest() {
		QueryRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("10"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.LT);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = client.query(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue((new Integer(item.get("date").getN())) < 1);
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionINTest() {
		QueryRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("5"));
		attributeValueList.add(new AttributeValue().withN("10"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.IN);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = client.query(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue((new Integer(item.get("date").getN())) <= 10 && (new Integer(item.get("date").getN())) >= 5);
		}
	}

	@Test
	public void queryWithHashKeyAndRangeKeyConditionBETWEENTest() {
		QueryRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("5"));
		attributeValueList.add(new AttributeValue().withN("10"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.BETWEEN);
		request.setRangeKeyCondition(rangeKeyCondition);
		QueryResult result = client.query(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue((new Integer(item.get("date").getN())) <= 10 && (new Integer(item.get("date").getN())) >= 5);
		}
	}

	@Test
	public void queryWithHashKeyAndLimitTest() {
		QueryRequest request = getBasicReq();
		request.setLimit(1);
		QueryResult result = client.query(request);
		Assert.assertNotNull(result);
		Assert.assertEquals(result.getCount().intValue(), 1);
	}

	@Test
	public void queryWithoutTableNameTest() {
		QueryRequest request = getBasicReq();
		request.setTableName(null);
		QueryResult result = client.query(request);
		Assert.assertNull(result.getItems());
	}*/
}

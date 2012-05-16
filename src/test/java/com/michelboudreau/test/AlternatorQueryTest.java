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

	private String testTableName;
	private int nbOfItems;
	private int numberOfSameHK;

	@Before
	public void setUp() throws Exception {
		testTableName = "Testing";
		nbOfItems = 60;
		numberOfSameHK = 10;
		client.createTable(new CreateTableRequest().withTableName("Testing").withKeySchema(new KeySchema().withHashKeyElement(getSSchema()).withRangeKeyElement(getNSchema())));
		client.batchWriteItem(generateItems());
	}

	@After
	public void tearDown() throws Exception {
		DeleteTableRequest del = new DeleteTableRequest();
		del.setTableName("Testing");
		client.deleteTable(del);
	}

	@Test
	public void queryWithHashKeyTest() {
		QueryRequest request = getBasicReq();
		request.setHashKeyValue(getHashKey());
		QueryResult result = client.query(request);
		Assert.assertNotNull(result);
		Assert.assertNotNull(result.getItems());
		Assert.assertNotNull(result.getItems().get(0));
		Assert.assertEquals(result.getCount().intValue(), 1);
		Assert.assertEquals(result.getItems().get(0).get("id"), getHashKey());
	}

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
	}

	public QueryRequest getBasicReq() {
		QueryRequest req = new QueryRequest();
		req.setTableName(testTableName);
		req.setHashKeyValue(getHashKey());
		return req;
	}


	public BatchWriteItemRequest generateItems() {
		BatchWriteItemRequest req = new BatchWriteItemRequest();
		Map<String, List<WriteRequest>> items = new HashMap<String, List<WriteRequest>>();
		List<WriteRequest> list = new ArrayList<WriteRequest>();
		for (int i = 0; i < nbOfItems; i++) {
			WriteRequest wreq = new WriteRequest();
			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
			item.put("id", new AttributeValue().withS("" + (i % (nbOfItems - numberOfSameHK))));
			item.put("date", new AttributeValue().withN(i + ""));
			item.put("testfield", new AttributeValue().withS(UUID.randomUUID().toString().substring(1, 4)));
			PutRequest putRequest = new PutRequest();
			putRequest.withItem(item);
			wreq.withPutRequest(putRequest);
			list.add(wreq);
		}
		items.put(testTableName, list);
		req.setRequestItems(items);
		return req;
	}

	public AttributeValue getHashKey() {
		return new AttributeValue().withS("1");
	}


	public KeySchemaElement getSSchema() {
		KeySchemaElement el = new KeySchemaElement();
		el.setAttributeName("id");
		el.setAttributeType(ScalarAttributeType.S);
		return el;
	}

	public KeySchemaElement getNSchema() {
		KeySchemaElement el = new KeySchemaElement();
		el.setAttributeName("date");
		el.setAttributeType(ScalarAttributeType.N);
		return el;
	}
}

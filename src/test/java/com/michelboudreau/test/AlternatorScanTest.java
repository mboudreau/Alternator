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
public class AlternatorScanTest extends AlternatorTest {

	private String testTableName;
	private int nbOfItems;
	private int numberOfRepeats;

	@Before
	public void setUp() {
		testTableName = "Testing";
		nbOfItems = 60;
		numberOfRepeats = 10;
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
	public void basicScanTest() {
		ScanRequest scanRequest = getBasicReq();
		ScanResult result = client.scan(scanRequest);
		Assert.assertNotNull(result);
		Assert.assertTrue(result.getCount().intValue() > 1);
	}

	@Test
	public void scanWithLimitTest() {
		ScanRequest scanRequest = getBasicReq();
		scanRequest.setLimit(5);
		ScanResult result = client.scan(scanRequest);
		Assert.assertNotNull(result);
		Assert.assertTrue(result.getCount().intValue() > 1 && result.getCount().intValue() <= 5);
	}


	@Test
	public void scanWithAttributesToGetTest() {
		ScanRequest request = getBasicReq();
		List<String> attrToGet = new ArrayList<String>();
		attrToGet.add("date");
		attrToGet.add("testfield");
		request.setAttributesToGet(attrToGet);
		ScanResult result = client.scan(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertFalse(item.containsKey("id"));
		}
	}

	@Test
	public void scanWithScanFilterEQTest() {
		ScanRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("1"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.EQ);
		Map<String, Condition> conditionMap = new HashMap<String, Condition>();
		conditionMap.put("date", rangeKeyCondition);
		request.setScanFilter(conditionMap);
		ScanResult result = client.scan(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertEquals(item.get("date").getN(), "1");
		}
	}

	@Test
	public void scanWithScanFilterGETest() {
		ScanRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("1"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.GE);
		Map<String, Condition> conditionMap = new HashMap<String, Condition>();
		conditionMap.put("date", rangeKeyCondition);
		request.setScanFilter(conditionMap);
		ScanResult result = client.scan(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(new Integer(item.get("date").getN()) >= 1);
		}
	}

	@Test
	public void scanWithScanFilterGTTest() {
		ScanRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("1"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.GT);
		Map<String, Condition> conditionMap = new HashMap<String, Condition>();
		conditionMap.put("date", rangeKeyCondition);
		request.setScanFilter(conditionMap);
		ScanResult result = client.scan(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(new Integer(item.get("date").getN()) > 1);
		}
	}

	@Test
	public void scanWithScanFilterLETest() {
		ScanRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("10"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.GT);
		Map<String, Condition> conditionMap = new HashMap<String, Condition>();
		conditionMap.put("date", rangeKeyCondition);
		request.setScanFilter(conditionMap);
		ScanResult result = client.scan(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(new Integer(item.get("date").getN()) <= 10);
		}
	}

	@Test
	public void scanWithScanFilterLTTest() {
		ScanRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("10"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.GT);
		Map<String, Condition> conditionMap = new HashMap<String, Condition>();
		conditionMap.put("date", rangeKeyCondition);
		request.setScanFilter(conditionMap);
		ScanResult result = client.scan(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(new Integer(item.get("date").getN()) < 10);
		}
	}

	@Test
	public void scanWithScanFilterINTest() {
		ScanRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("10"));
		attributeValueList.add(new AttributeValue().withN("20"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.IN);
		Map<String, Condition> conditionMap = new HashMap<String, Condition>();
		conditionMap.put("date", rangeKeyCondition);
		request.setScanFilter(conditionMap);
		ScanResult result = client.scan(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(new Integer(item.get("date").getN()) <= 20 && new Integer(item.get("date").getN()) >= 10);
		}
	}

	@Test
	public void scanWithScanFilterBETWEENTest() {
		ScanRequest request = getBasicReq();
		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withN("10"));
		attributeValueList.add(new AttributeValue().withN("20"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.BETWEEN);
		Map<String, Condition> conditionMap = new HashMap<String, Condition>();
		conditionMap.put("date", rangeKeyCondition);
		request.setScanFilter(conditionMap);
		ScanResult result = client.scan(request);
		Assert.assertNotNull(result);
		for (Map<String, AttributeValue> item : result.getItems()) {
			Assert.assertTrue(new Integer(item.get("date").getN()) <= 20 && new Integer(item.get("date").getN()) >= 10);
		}
	}

	@Test
	public void scanPaginationTest() {
		Key lastKeyEvaluated = null;
		do {
			ScanRequest scanRequest = getBasicReq()
					.withLimit(10)
					.withExclusiveStartKey(lastKeyEvaluated);

			ScanResult result = client.scan(scanRequest);
			for (Map<String, AttributeValue> item : result.getItems()) {
				Assert.assertNotNull(result.getLastEvaluatedKey());
			}
			lastKeyEvaluated = result.getLastEvaluatedKey();
		} while (lastKeyEvaluated != null);
	}

	@Test
	public void scanWithoutTableNameTest() {
		ScanRequest request = getBasicReq();
		request.setTableName(null);
		ScanResult result = client.scan(request);
		Assert.assertNull(result.getItems());
	}

	public ScanRequest getBasicReq() {
		ScanRequest req = new ScanRequest();
		req.setTableName(testTableName);
		return req;
	}

	public BatchWriteItemRequest generateItems() {
		BatchWriteItemRequest req = new BatchWriteItemRequest();
		Map<String, List<WriteRequest>> items = new HashMap<String, List<WriteRequest>>();
		List<WriteRequest> list = new ArrayList<WriteRequest>();
		for (int i = 0; i < nbOfItems; i++) {
			WriteRequest wreq = new WriteRequest();
			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
			item.put("id", new AttributeValue().withS("" + (i % (nbOfItems - numberOfRepeats))));
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

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

    private String tableName;
    private int nbOfItems;
    private int numberOfRepeats;
    private AttributeValue rangeKey;

    @Before
    public void setUp() {
        tableName = createTableName();
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        nbOfItems = 60;
        numberOfRepeats = 10;
        rangeKey = createStringAttribute();
        for (int i = 0; i < (nbOfItems); i++) {
            putItemInDb();
        }
        for (int i = 0; i < (numberOfRepeats); i++) {
            putItemInDb(rangeKey);
        }
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
        Assert.assertNotNull(result.getCount());
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

    protected ScanRequest getBasicReq() {
        ScanRequest req = new ScanRequest();
        req.setTableName(tableName);
        return req;
    }

    protected AttributeValue putItemInDb() {
        AttributeValue hash = createStringAttribute();
        Map<String, AttributeValue> item = createGenericItem(hash);
        PutItemRequest req = new PutItemRequest().withTableName(tableName).withItem(item);
        client.putItem(req);
        return hash;
    }

    protected AttributeValue putItemInDb(AttributeValue rangeKey) {
        AttributeValue hash = createStringAttribute();
        Map<String, AttributeValue> item = createGenericItem(hash, rangeKey);
        PutItemRequest req = new PutItemRequest().withTableName(tableName).withItem(item);
        client.putItem(req);
        return hash;
    }

    protected AttributeValue createStringAttribute() {
        return new AttributeValue(UUID.randomUUID().toString());
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

    protected String createTableName() {
        return "Table" + UUID.randomUUID().toString().substring(0, 4);
    }
}

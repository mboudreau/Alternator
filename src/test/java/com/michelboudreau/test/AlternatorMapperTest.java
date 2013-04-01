package com.michelboudreau.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ResourceInUseException;
import com.amazonaws.services.dynamodb.model.ScalarAttributeType;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations =
{
    "classpath:/applicationContext.xml"
})
public class AlternatorMapperTest extends AlternatorTest
{
    private final String hashTableName = "mapper.TestClassWithHashKey";
    private final String hashRangeTableName = "mapper.TestClassWithRangeHashKey";

    private DynamoDBMapper mapper;

    private DynamoDBMapper createMapper()
    {
        return new DynamoDBMapper(getClient(), createMapperConfiguration());
    }

    private DynamoDBMapperConfig createMapperConfiguration()
    {
        return new DynamoDBMapperConfig(
                DynamoDBMapperConfig.SaveBehavior.CLOBBER,
                DynamoDBMapperConfig.ConsistentReads.CONSISTENT,
                null);
    }

    @Before
    public void setUp() throws Exception
    {
        mapper = createMapper();
    }

    @After
    public void tearDown() throws Exception
    {
        deleteAllTables();
    }

    //Test: put item with HashKey
    @Test
    public void putItemWithHashKey()
    {
        KeySchema schema =
                new KeySchema(
                    new KeySchemaElement().withAttributeName("code").withAttributeType(ScalarAttributeType.S)
                );
        createTable(hashTableName, schema);

        TestClassWithHashKey value = new TestClassWithHashKey();
        value.setCode("hash1");
        value.setStringData("string1");
        value.setIntData(1);
        mapper.save(value);
    }

    @Test
    public void putItemWithHashKeyOverwriteItem()
    {
        KeySchema schema =
                new KeySchema(
                    new KeySchemaElement().withAttributeName("code").withAttributeType(ScalarAttributeType.S)
                );
		try {
        createTable(hashTableName, schema);
		} catch (ResourceInUseException riue) {
			// The table is already created, do nothing
		}

        TestClassWithHashKey value2a = new TestClassWithHashKey();
        value2a.setCode("hash2");
        value2a.setStringData("string2a");
        value2a.setIntData(21);
        mapper.save(value2a);

        TestClassWithHashKey value2b = new TestClassWithHashKey();
        value2b.setCode("hash2");
        value2b.setStringData("string2b");
        value2b.setIntData(22);
        mapper.save(value2b);
    }

    @Test
    public void putItemWithHashKeyAndRangeKey()
    {
        KeySchema schema =
                new KeySchema(
                    new KeySchemaElement().withAttributeName("hashCode").withAttributeType(ScalarAttributeType.S)
                );
        schema.setRangeKeyElement(new KeySchemaElement().withAttributeName(
                "rangeCode").withAttributeType(ScalarAttributeType.S));
        createTable(hashRangeTableName, schema);

        TestClassWithHashRangeKey value = new TestClassWithHashRangeKey();
        value.setHashCode("hash1");
        value.setRangeCode("range1");
        value.setStringData("string1");
        value.setIntData(1);
        mapper.save(value);
    }

    @Test
    public void putItemWithHashKeyAndRangeKeyOverwriteItem()
    {
        KeySchema schema =
                new KeySchema(
                    new KeySchemaElement().withAttributeName("hashCode").withAttributeType(ScalarAttributeType.S)
                );
        schema.setRangeKeyElement(new KeySchemaElement().withAttributeName(
                "rangeCode").withAttributeType(ScalarAttributeType.S));
		try {
			createTable(hashRangeTableName, schema);
		} catch (ResourceInUseException riue) {
			// The table is already created
		}

        TestClassWithHashRangeKey value2a = new TestClassWithHashRangeKey();
        value2a.setHashCode("hash2");
        value2a.setRangeCode("range2");
        value2a.setStringData("string2a");
        value2a.setIntData(21);
        mapper.save(value2a);

        TestClassWithHashRangeKey value2b = new TestClassWithHashRangeKey();
        value2b.setHashCode("hash2");
        value2b.setRangeCode("range2");
        value2b.setStringData("string2b");
        value2b.setIntData(22);
        mapper.save(value2b);
    }

    @Test
    public void getHashItemTest()
    {
        putItemWithHashKey();

        String code = "hash1";
        TestClassWithHashKey value = mapper.load(TestClassWithHashKey.class, code);
        Assert.assertNotNull("Value not found.", value);
        Assert.assertEquals("Wrong code.", code, value.getCode());
        Assert.assertEquals("Wrong stringData.", "string1", value.getStringData());
        Assert.assertEquals("Wrong intData.", 1, value.getIntData());
    }

    @Test
    public void getUnknownHashItemTest()
    {
		KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("code").withAttributeType(ScalarAttributeType.S));
		createTable(hashTableName, schema);

        String code = "hash1x";
        TestClassWithHashKey value = mapper.load(TestClassWithHashKey.class, code);
        Assert.assertNull("Value should not be found.", value);
    }

    @Test
    public void getHashRangeItemTest()
    {
        putItemWithHashKeyAndRangeKey();
        putItemWithHashKeyAndRangeKeyOverwriteItem();

        TestClassWithHashRangeKey value2c = new TestClassWithHashRangeKey();
        value2c.setHashCode("hash2");
        value2c.setRangeCode("range2c");
        value2c.setStringData("string2c");
        value2c.setIntData(23);
        mapper.save(value2c);

        String hashCode = "hash2";
        String rangeCode = "range2";
        TestClassWithHashRangeKey value = mapper.load(TestClassWithHashRangeKey.class, hashCode, rangeCode);
        Assert.assertNotNull("Value not found.", value);
        Assert.assertEquals("Wrong hashCode.", hashCode, value.getHashCode());
        Assert.assertEquals("Wrong rangeCode.", rangeCode, value.getRangeCode());
        Assert.assertEquals("Wrong stringData.", "string2b", value.getStringData());
        Assert.assertEquals("Wrong intData.", 22, value.getIntData());
    }

    @Test
    public void getUnknownHashRangeItemTest()
    {
		KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("hashCode").withAttributeType(ScalarAttributeType.S));
		schema.setRangeKeyElement(new KeySchemaElement().withAttributeName("rangeCode").withAttributeType(ScalarAttributeType.S));
		createTable(hashRangeTableName, schema);

        String hashCode = "hash2x";
        String rangeCode = "range2";
        TestClassWithHashRangeKey value = mapper.load(TestClassWithHashRangeKey.class, hashCode, rangeCode);
        Assert.assertNull("Value should not be found (" + hashCode + "/" + rangeCode, value);

        hashCode = "hash2";
        rangeCode = "range2x";
        value = mapper.load(TestClassWithHashRangeKey.class, hashCode, rangeCode);
        Assert.assertNull("Value should not be found (" + hashCode + "/" + rangeCode, value);
    }

	@Test
	public void queryWithHashKey() {
        putItemWithHashKey();
        putItemWithHashKeyOverwriteItem();

        String code = "hash1";

        DynamoDBQueryExpression query =
                new DynamoDBQueryExpression(new AttributeValue().withS(code));

        List<TestClassWithHashKey> valueList = mapper.query(TestClassWithHashKey.class, query);
        Assert.assertNotNull("Value list is null.", valueList);
        Assert.assertNotSame("Value list is empty.", 0, valueList.size());

        Assert.assertEquals("Value list has more than one item.", 1, valueList.size());

        TestClassWithHashKey value = valueList.get(0);
        Assert.assertEquals("Wrong code.", code, value.getCode());
        Assert.assertEquals("Wrong stringData.", "string1", value.getStringData());
        Assert.assertEquals("Wrong intData.", 1, value.getIntData());
	}

	@Test
	public void queryWithUnknownHashKey() {
        putItemWithHashKey();

        String code = "hash1x";

        DynamoDBQueryExpression query =
                new DynamoDBQueryExpression(new AttributeValue().withS(code));

        List<TestClassWithHashKey> valueList = mapper.query(TestClassWithHashKey.class, query);
        Assert.assertNotNull("Value list is null.", valueList);
        Assert.assertEquals("Value list should be empty.", 0, valueList.size());
	}

	@Test
	public void queryWithHashRangeKey() {
        putItemWithHashKeyAndRangeKey();

        TestClassWithHashRangeKey value2c = new TestClassWithHashRangeKey();
        value2c.setHashCode("hash2");
        value2c.setRangeCode("range2c");
        value2c.setStringData("string2c");
        value2c.setIntData(23);
        mapper.save(value2c);

        TestClassWithHashRangeKey value2d = new TestClassWithHashRangeKey();
        value2d.setHashCode("hash2");
        value2d.setRangeCode("range2d");
        value2d.setStringData("string2d");
        value2d.setIntData(24);
        mapper.save(value2d);

        TestClassWithHashRangeKey value2e = new TestClassWithHashRangeKey();
        value2e.setHashCode("hash2");
        value2e.setRangeCode("range2e");
        value2e.setStringData("string2e");
        value2e.setIntData(25);
        mapper.save(value2e);

        String hashCode = "hash2";

        DynamoDBQueryExpression query =
                new DynamoDBQueryExpression(new AttributeValue().withS(hashCode));

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withS("range2c"));
		attributeValueList.add(new AttributeValue().withS("range2d"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.BETWEEN);
		query.setRangeKeyCondition(rangeKeyCondition);

        List<TestClassWithHashRangeKey> valueList = mapper.query(TestClassWithHashRangeKey.class, query);
        Assert.assertNotNull("Value list is null.", valueList);
        Assert.assertNotSame("Value list is empty.", 0, valueList.size());

        Assert.assertEquals("Value list should have 2 items.", 2, valueList.size());

        TestClassWithHashRangeKey value = valueList.get(0);
        Assert.assertEquals("Wrong hashCode.", hashCode, value.getHashCode());
        Assert.assertEquals("Wrong rangeCode.", "range2c", value.getRangeCode());
        Assert.assertEquals("Wrong stringData.", "string2c", value.getStringData());
        Assert.assertEquals("Wrong intData.", 23, value.getIntData());

        value = valueList.get(1);
        Assert.assertEquals("Wrong hashCode.", hashCode, value.getHashCode());
        Assert.assertEquals("Wrong rangeCode.", "range2d", value.getRangeCode());
        Assert.assertEquals("Wrong stringData.", "string2d", value.getStringData());
        Assert.assertEquals("Wrong intData.", 24, value.getIntData());
	}

	@Test
	public void queryWithUnknownHashRangeKey1() {
        putItemWithHashKeyAndRangeKey();

        String hashCode = "hash1x";

        DynamoDBQueryExpression query =
                new DynamoDBQueryExpression(new AttributeValue().withS(hashCode));

        List<TestClassWithHashRangeKey> valueList = mapper.query(TestClassWithHashRangeKey.class, query);
        Assert.assertNotNull("Value list is null.", valueList);
        Assert.assertEquals("Value list should be empty.", 0, valueList.size());
	}

	@Test
	public void queryWithUnknownHashRangeKey2() {
        putItemWithHashKeyAndRangeKey();

        String hashCode = "hash2";

        DynamoDBQueryExpression query =
                new DynamoDBQueryExpression(new AttributeValue().withS(hashCode));

		Condition rangeKeyCondition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withS("range2x"));
		attributeValueList.add(new AttributeValue().withS("range2y"));
		rangeKeyCondition.setAttributeValueList(attributeValueList);
		rangeKeyCondition.setComparisonOperator(ComparisonOperator.BETWEEN);
		query.setRangeKeyCondition(rangeKeyCondition);

        List<TestClassWithHashRangeKey> valueList = mapper.query(TestClassWithHashRangeKey.class, query);
        Assert.assertNotNull("Value list is null.", valueList);
        Assert.assertEquals("Value list should be empty.", 0, valueList.size());
	}

    @Test
    public void deleteHashItemTest()
    {
		putItemWithHashKey();
		putItemWithHashKeyOverwriteItem();

        String code = "hash1";
        TestClassWithHashKey value = mapper.load(TestClassWithHashKey.class, code);
        Assert.assertNotNull("Value not found.", value);
        Assert.assertEquals("Wrong code.", code, value.getCode());
        Assert.assertEquals("Wrong stringData.", "string1", value.getStringData());
        Assert.assertEquals("Wrong intData.", 1, value.getIntData());

        mapper.delete(value);

        TestClassWithHashKey value2 = mapper.load(TestClassWithHashKey.class, code);
        Assert.assertNull("Value2 should not be found.", value2);
    }

    @Test
    public void deleteHashRangeItemTest()
    {
        putItemWithHashKeyAndRangeKey();
		putItemWithHashKeyAndRangeKeyOverwriteItem();

        String hashCode = "hash2";
        String rangeCode = "range2";
        TestClassWithHashRangeKey value = mapper.load(TestClassWithHashRangeKey.class, hashCode, rangeCode);
        Assert.assertNotNull("Value not found.", value);
        Assert.assertEquals("Wrong hashCode.", hashCode, value.getHashCode());
        Assert.assertEquals("Wrong rangeCode.", rangeCode, value.getRangeCode());
        Assert.assertEquals("Wrong stringData.", "string2b", value.getStringData());
        Assert.assertEquals("Wrong intData.", 22, value.getIntData());

        mapper.delete(value);

        TestClassWithHashRangeKey value2 = mapper.load(TestClassWithHashRangeKey.class, hashCode, rangeCode);
        Assert.assertNull("Value2 should not be found.", value2);
    }

	@Test
	public void scanIndexForwardFalseTest() {
		KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("hashCode").withAttributeType(ScalarAttributeType.S));
		schema.setRangeKeyElement(new KeySchemaElement().withAttributeName("rangeCode").withAttributeType(ScalarAttributeType.S));
		createTable(hashRangeTableName, schema);

		{
			TestClassWithHashRangeKey c = new TestClassWithHashRangeKey();
			c.setHashCode("code");
			c.setRangeCode("1");
			c.setStringData("first");
			mapper.save(c);
		}

		{
			TestClassWithHashRangeKey c = new TestClassWithHashRangeKey();
			c.setHashCode("code");
			c.setRangeCode("2");
			c.setStringData("second");
			mapper.save(c);
		}
		TestClassWithHashRangeKey res = mapper.query(TestClassWithHashRangeKey.class, new DynamoDBQueryExpression(new AttributeValue("code")).withScanIndexForward(false).withLimit(1)).get(0);
		Assert.assertEquals("second", res.getStringData());
	}

	@Test
	public void limitTest() {
		KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("hashCode").withAttributeType(ScalarAttributeType.S));
		schema.setRangeKeyElement(new KeySchemaElement().withAttributeName("rangeCode").withAttributeType(ScalarAttributeType.S));
		createTable(hashRangeTableName, schema);

		for (int i = 0; i < 10; i++) {
			TestClassWithHashRangeKey c = new TestClassWithHashRangeKey();
			c.setHashCode("code");
			c.setRangeCode(i + "");
			mapper.save(c);
		}

		Assert.assertEquals(1, mapper.query(TestClassWithHashRangeKey.class, new DynamoDBQueryExpression(new AttributeValue("code")).withLimit(1)).size());
		Assert.assertEquals(3, mapper.query(TestClassWithHashRangeKey.class, new DynamoDBQueryExpression(new AttributeValue("code")).withLimit(3)).size());
		Assert.assertEquals(10, mapper.query(TestClassWithHashRangeKey.class, new DynamoDBQueryExpression(new AttributeValue("code")).withLimit(20)).size());
	}

	@Test
	public void utf8Test() {
		KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("code").withAttributeType(ScalarAttributeType.S));
		createTable(hashTableName, schema);

		TestClassWithHashKey value = new TestClassWithHashKey();
		value.setCode("éáűőúöüóí");
		value.setStringData("űáéúőóüöí");
		mapper.save(value);

		TestClassWithHashKey readValue = mapper.load(TestClassWithHashKey.class, "éáűőúöüóí");
		Assert.assertEquals("éáűőúöüóí", readValue.getCode());
		Assert.assertEquals("űáéúőóüöí", readValue.getStringData());

	}
}

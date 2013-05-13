package com.michelboudreau.testv2;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternatorv2.AlternatorDBClientV2;
import com.michelboudreau.alternatorv2.AlternatorDBInProcessClientV2;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.inject.Inject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.util.Assert;

public class AlternatorTest {
    /**
     * Set to true to run AlternatorDBHandler as a separate Jetty service process.
     * Set to false to invoke AlternatorDBHandler methods in-process
     * to facilitate breakpoint debugging.
     */
	static boolean RUN_DB_AS_SERVICE = true;

    /**
     * Set to true to spawn the service in a local sub-process.
     * Set to false if a standalone executable JAR instance of Alternator is running in another process.
     */
    private static final boolean SPAWN_LOCAL_DB_SERVICE = true;

	static protected AlternatorDBClientV2 client;
	static protected DynamoDBMapper mapper;

    static protected AlternatorDBInProcessClientV2 inProcessClient;
    static protected DynamoDBMapper inProcessMapper;

	static protected AlternatorDB db;

	private ProvisionedThroughput provisionedThroughput;

	public AlternatorTest() {
		provisionedThroughput = new ProvisionedThroughput();
		provisionedThroughput.setReadCapacityUnits(10L);
		provisionedThroughput.setWriteCapacityUnits(10L);
	}

	@BeforeClass
	public static void setUpOnce() throws Exception {
        if (RUN_DB_AS_SERVICE && SPAWN_LOCAL_DB_SERVICE) {
            db = new AlternatorDB().start();
        }
	}

	@AfterClass
	public static void tearDownOnce() throws Exception {
        if (db != null) {
            db.stop();
        }
	}

    @Inject
    public void setMapper(DynamoDBMapper value) {
        mapper = value;
    }

	@Inject
	public void setClient(AlternatorDBClientV2 value) {
		client = value;
	}

    @Test
    public void noOpTest() {
        Assert.isTrue(true);
    }

    protected AmazonDynamoDB getClient() {
        if (RUN_DB_AS_SERVICE) {
            return client;
        } else {
            if (inProcessClient == null) {
                inProcessClient = new AlternatorDBInProcessClientV2();
            }
            return inProcessClient;
        }
    }

    protected DynamoDBMapper getMapper() {
        if (RUN_DB_AS_SERVICE) {
            return mapper;
        } else {
            if (inProcessMapper == null) {
                inProcessMapper = new DynamoDBMapper(getClient());
            }
            return inProcessMapper;
        }
    }

	protected AttributeDefinition createStringAttributeDefinition() {
		String name = UUID.randomUUID().toString().substring(0, 2);
        return createStringAttributeDefinition(name);
	}

	protected AttributeDefinition createStringAttributeDefinition(String name) {
        return createAttributeDefinition(name, ScalarAttributeType.S);
	}

	protected AttributeDefinition createNumberAttributeDefinition() {
		String name = UUID.randomUUID().toString().substring(0, 2);
        return createNumberAttributeDefinition(name);
	}

	protected AttributeDefinition createNumberAttributeDefinition(String name) {
        return createAttributeDefinition(name, ScalarAttributeType.N);
	}

	protected AttributeDefinition createAttributeDefinition(
            String name,
            ScalarAttributeType type) {

		AttributeDefinition attr = new AttributeDefinition();
		attr.setAttributeName(name);
		attr.setAttributeType(type);
		return attr;
	}

	protected List<KeySchemaElement> createKeySchema(
            AttributeDefinition hashAttr,
            AttributeDefinition rangeAttr) {

        List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        if (hashAttr != null) {
            keySchema.add(
                new KeySchemaElement()
                    .withAttributeName(hashAttr.getAttributeName())
                    .withKeyType(KeyType.HASH)
                    );
        }
        if (rangeAttr != null) {
            keySchema.add(
                new KeySchemaElement()
                    .withAttributeName(rangeAttr.getAttributeName())
                    .withKeyType(KeyType.RANGE)
                    );
        }
		return keySchema;
	}

	protected List<AttributeDefinition> createKeyAttributes(
            AttributeDefinition hashAttr,
            AttributeDefinition rangeAttr) {

        List<AttributeDefinition> attrList = new ArrayList<AttributeDefinition>();
        if (hashAttr != null) {
            attrList.add(hashAttr);
        }
        if (rangeAttr != null) {
            attrList.add(rangeAttr);
        }
		return attrList;
	}

    protected KeySchemaElement getHashKeyElement(List<KeySchemaElement> schema) {
        KeySchemaElement hashElement = new KeySchemaElement().withAttributeName("");
        for (KeySchemaElement element : schema) {
            if (element.getKeyType().equals(KeyType.HASH.toString())) {
                hashElement = element;
                break; // out of 'for'
            }
        }
        return hashElement;
    }

    protected KeySchemaElement getRangeKeyElement(List<KeySchemaElement> schema) {
        KeySchemaElement rangeElement = new KeySchemaElement().withAttributeName("");
        for (KeySchemaElement element : schema) {
            if (element.getKeyType().equals(KeyType.RANGE.toString())) {
                rangeElement = element;
                break; // out of 'for'
            }
        }
        return rangeElement;
    }

	protected Map<String, AttributeValue> createItemKey(
            String hashName,
            AttributeValue hashValue) {

        return createItemKey(hashName, hashValue, null, null);
	}

	protected Map<String, AttributeValue> createItemKey(
            String hashName,
            AttributeValue hashValue,
            String rangeName,
            AttributeValue rangeValue) {

        Map<String, AttributeValue> attrMap = new HashMap<String, AttributeValue>();
        if (hashName != null) {
            attrMap.put(hashName, hashValue);
        }
        if (rangeName != null) {
            attrMap.put(rangeName, rangeValue);
        }
		return attrMap;
	}

	protected String createTableName() {
		return "Table" + UUID.randomUUID().toString().substring(0, 4);
	}

	protected TableDescription createTable() {
		return createTable(createTableName());
	}

	protected TableDescription createTable(String name) {
		return createTable(name, createStringAttributeDefinition());
	}

	protected TableDescription createTable(AttributeDefinition hashAttr) {
		return createTable(createTableName(), hashAttr);
	}

	protected TableDescription createTable(
            String name,
            AttributeDefinition hashAttr) {
		return createTable(name, hashAttr, null);
	}

	protected TableDescription createTable(
            String name,
            AttributeDefinition hashAttr,
            AttributeDefinition rangeAttr) {
		return createTable(name, hashAttr, rangeAttr, provisionedThroughput);
	}

	protected TableDescription createTable(
            String name,
            AttributeDefinition hashAttr,
            AttributeDefinition rangeAttr,
            ProvisionedThroughput throughput) {

        CreateTableRequest request =
            new CreateTableRequest()
                .withTableName(name)
                .withKeySchema(createKeySchema(hashAttr, rangeAttr))
                .withAttributeDefinitions(createKeyAttributes(hashAttr, rangeAttr))
                .withProvisionedThroughput(throughput)
                ;
        CreateTableResult result = getClient().createTable(request);
        TableDescription tableDesc = result.getTableDescription();
        return tableDesc;
	}

    protected void createGenericTable(String tableName) {
        createGenericTable(tableName, "id");
    }

    protected void createGenericTable(String tableName, String hashKeyName) {
        AttributeDefinition hashAttr = createStringAttributeDefinition(hashKeyName);
        createTable(tableName, hashAttr);
    }

    protected void createGenericHashRangeTable(String tableName) {
        createGenericHashRangeTable(tableName, "id", "range");
    }

    protected void createGenericHashRangeTable(String tableName, String hashKeyName, String rangeKeyName) {
        AttributeDefinition hashAttr = createStringAttributeDefinition(hashKeyName);
        AttributeDefinition rangeAttr = createStringAttributeDefinition(rangeKeyName);
        createTable(tableName, hashAttr, rangeAttr);
    }

	protected void deleteAllTables() {
		String lastTableName = null;
		while (true) {
			ListTablesResult res = getClient().listTables(new ListTablesRequest().withExclusiveStartTableName(lastTableName));
			for (String tableName : res.getTableNames()) {
				getClient().deleteTable(new DeleteTableRequest(tableName));
			}
			lastTableName = res.getLastEvaluatedTableName();
			if (lastTableName == null) {
				break;
			}
		}
	}

	protected AttributeValue createStringAttribute() {
		return new AttributeValue(UUID.randomUUID().toString());
	}

	protected AttributeValue createStringAttribute(String value) {
		return new AttributeValue(value);
	}

	protected AttributeValue createNumberAttribute() {
		return new AttributeValue().withN(Math.round(Math.random() * 1000)+"");
	}

	protected AttributeValue createNumberAttribute(Integer value) {
		return new AttributeValue().withN(value.toString());
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
		if(range != null) {
			map.put("range", range);
		}
		return map;
	}

	protected Map<String, AttributeValue> createGenericItem(
            AttributeValue hash,
            AttributeValue range,
            String attrName, String attrValue) {
        return createGenericItem(hash, range, attrName, attrValue, null, null);
	}

	protected Map<String, AttributeValue> createGenericItem(
            AttributeValue hash,
            AttributeValue range,
            String attrName1, String attrValue1,
            String attrName2, String attrValue2) {
		Map<String, AttributeValue> map = new HashMap<String, AttributeValue>();
		map.put("id", hash);
		if(range != null) {
			map.put("range", range);
		}
		if((attrName1 != null) && (attrValue1 != null)) {
			map.put(attrName1, createStringAttribute(attrValue1));
		}
		if((attrName2 != null) && (attrValue2 != null)) {
			map.put(attrName2, createStringAttribute(attrValue2));
		}
		return map;
	}

    protected AttributeValue createItem(String tableName){
        createGenericTable(tableName);

        AttributeValue hash = createStringAttribute();
        Map<String, AttributeValue> item = createGenericItem(hash);

        PutItemRequest req = new PutItemRequest().withTableName(tableName).withItem(item);
        getClient().putItem(req);

        return hash;
    }

    protected Map<String, Condition> createHashKeyCondition(
            String hashKeyName,
            AttributeValue hashKeyValue) {

        List<AttributeValue> keyValues = new ArrayList<AttributeValue>();
        keyValues.add(hashKeyValue);

        Map<String, Condition> keyConditions = new HashMap<String, Condition>();
        Condition hashKeyCondition =
            new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(keyValues)
                ;
        keyConditions.put(hashKeyName, hashKeyCondition);

        return keyConditions;
    }
}

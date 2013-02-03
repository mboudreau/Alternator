package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodb.model.*;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternator.AlternatorDBClient;
import com.michelboudreau.alternator.AlternatorDBInProcessClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AlternatorTest {
    /**
     * Set to true to run AlternatorDBHandler as a separate Jetty service process.
     * Set to false to invoke AlternatorDBHandler methods in-process
     * to facilitate breakpoint debugging.
     */
    private static final boolean RUN_DB_AS_SERVICE = true;

	static protected AlternatorDBClient client;
	static protected DynamoDBMapper mapper;

    static protected AlternatorDBInProcessClient inProcessClient;
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
        if (RUN_DB_AS_SERVICE) {
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
	public void setClient(AlternatorDBClient value) {
		client = value;
	}

    protected AmazonDynamoDB getClient() {
        if (RUN_DB_AS_SERVICE) {
            return client;
        } else {
            if (inProcessClient == null) {
                inProcessClient = new AlternatorDBInProcessClient();
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

	protected KeySchemaElement createStringKeyElement() {
		KeySchemaElement el = new KeySchemaElement();
		el.setAttributeName(UUID.randomUUID().toString().substring(0, 2));
		el.setAttributeType(ScalarAttributeType.S);
		return el;
	}

	protected KeySchemaElement createNumberKeyElement() {
		KeySchemaElement el = createStringKeyElement();
		el.setAttributeType(ScalarAttributeType.N);
		return el;
	}

	protected KeySchema createKeySchema() {
		return createKeySchema(createStringKeyElement(), null);
	}

	protected KeySchema createKeySchema(KeySchemaElement hashKey) {
		return createKeySchema(hashKey, null);
	}

	protected KeySchema createKeySchema(KeySchemaElement hashKey, KeySchemaElement rangeKey) {
		KeySchema schema = new KeySchema(hashKey);
		schema.setRangeKeyElement(rangeKey);
		return schema;
	}

	protected String createTableName() {
		return "Table" + UUID.randomUUID().toString().substring(0, 4);
	}

	protected TableDescription createTable() {
		return createTable(createTableName(), createKeySchema(), provisionedThroughput);
	}

	protected TableDescription createTable(String name) {
		return createTable(name, createKeySchema(), provisionedThroughput);
	}

	protected TableDescription createTable(KeySchema schema) {
		return createTable(createTableName(), schema, provisionedThroughput);
	}

	protected TableDescription createTable(String name, KeySchema schema) {
		return createTable(name, schema, provisionedThroughput);
	}

	protected TableDescription createTable(String name, KeySchema schema, ProvisionedThroughput throughput) {
		return getClient().createTable(new CreateTableRequest(name, schema).withProvisionedThroughput(throughput)).getTableDescription();
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
        KeySchema schema = new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S));
        createTable(tableName, schema);
        AttributeValue hash = createStringAttribute();
        Map<String, AttributeValue> item = createGenericItem(hash);
        PutItemRequest req = new PutItemRequest().withTableName(tableName).withItem(item);
        getClient().putItem(req);
        return hash;
    }
}

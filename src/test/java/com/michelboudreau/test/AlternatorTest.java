package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodb.model.*;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternator.AlternatorDBClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;

public class AlternatorTest {
	static protected AlternatorDBClient client;
	static protected DynamoDBMapper mapper;
	static protected AlternatorDB db;

	private ProvisionedThroughput provisionedThroughput;

	public AlternatorTest() {
		provisionedThroughput = new ProvisionedThroughput();
		provisionedThroughput.setReadCapacityUnits(10L);
		provisionedThroughput.setWriteCapacityUnits(10L);
	}

	@BeforeClass
	public static void setUpOnce() throws Exception {
		db = new AlternatorDB().start();
	}

	@AfterClass
	public static void tearDownOnce() throws Exception {
		db.stop();
	}

	@Autowired
	public void setClient(AlternatorDBClient value) {
		client = value;
	}

	@Autowired
	public void setMapper(DynamoDBMapper value) {
		mapper = value;
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
		return client.createTable(new CreateTableRequest(name, schema).withProvisionedThroughput(throughput)).getTableDescription();
	}

	protected void deleteAllTables() {
		String lastTableName = null;
		while (true) {
			ListTablesResult res = client.listTables(new ListTablesRequest().withExclusiveStartTableName(lastTableName));
			for (String tableName : res.getTableNames()) {
				client.deleteTable(new DeleteTableRequest(tableName));
			}
			lastTableName = res.getLastEvaluatedTableName();
			if (lastTableName == null) {
				break;
			}
		}
	}
}

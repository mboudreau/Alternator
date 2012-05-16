package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.model.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Date;
import java.util.UUID;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext.xml"})
public class AlternatorTableTest extends AlternatorTest {

	private ProvisionedThroughput provisionedThroughput;

	@Before
	public void setUp() {
		provisionedThroughput = new ProvisionedThroughput();
		provisionedThroughput.setReadCapacityUnits(10L);
		provisionedThroughput.setWriteCapacityUnits(10L);
	}

	@After
	public void tearDown() {
		deleteAllTables();
	}

	@Test
	public void createTableWithStringHashKey() {
		String name = createTableName();
		TableDescription res = createTable(name, createKeySchema(createStringKeyElement()));
		Assert.assertNotNull(res);
		Assert.assertEquals(res.getTableName(), name);
	}

	@Test
	public void createTableWithNumberHashKey() {
		String name = createTableName();
		TableDescription res = createTable(name, createKeySchema(createNumberKeyElement()));
		Assert.assertNotNull(res);
		Assert.assertEquals(res.getTableName(), name);
	}

	@Test
	public void createTableWithStringHashKeyAndStringRangeKey() {
		String name = createTableName();
		TableDescription res = createTable(name, createKeySchema(createStringKeyElement(), createStringKeyElement()));
		Assert.assertNotNull(res);
		Assert.assertEquals(res.getTableName(), name);
	}


	@Test
	public void createTableWithStringHashKeyAndNumberRangeKey() {
		String name = createTableName();
		TableDescription res = createTable(name, createKeySchema(createStringKeyElement(), createNumberKeyElement()));
		Assert.assertNotNull(res);
		Assert.assertEquals(res.getTableName(), name);
	}

	@Test
	public void createTableWithNumberHashKeyAndStringRangeKey() {
		String name = createTableName();
		TableDescription res = createTable(name, createKeySchema(createNumberKeyElement(), createStringKeyElement()));
		Assert.assertNotNull(res);
		Assert.assertEquals(res.getTableName(), name);
	}

	@Test
	public void createTableWithNumberHashKeyAndNumberRangeKey() {
		String name = createTableName();
		TableDescription res = createTable(name, createKeySchema(createNumberKeyElement(), createNumberKeyElement()));
		Assert.assertNotNull(res);
		Assert.assertEquals(res.getTableName(), name);
	}

	@Test
	public void createTableWithoutHashKey() {
		Assert.assertNull(createTable(createKeySchema(null, createNumberKeyElement())));
	}

	@Test
	public void createTableWithoutName() {
		Assert.assertNull(createTable(null, createKeySchema()));
	}

	@Test
	public void createTableWithoutThroughput() {
		Assert.assertNull(createTable(createTableName(), createKeySchema(), null));
	}

	@Test
	public void createTableWithSameHashAndRangeKey() {
		KeySchemaElement el = createStringKeyElement();
		Assert.assertNull(createTable(createKeySchema(el, el)));
	}

	@Test
	public void describeTable() {
		String name = createTableName();
		createTable(name);
		DescribeTableResult res = client.describeTable(new DescribeTableRequest().withTableName(name));
		Assert.assertNotNull(res.getTable());
		Assert.assertEquals(res.getTable().getTableName(), name);
	}

	@Test
	public void describeTableWithoutTableName() {
		createTable();
		DescribeTableResult res = client.describeTable(new DescribeTableRequest());
		Assert.assertNull(res.getTable());
	}

	@Test
	public void listTables() {
		String name = createTableName();
		createTable(name);
		ListTablesResult res = client.listTables();
		Assert.assertTrue(res.getTableNames().contains(name));
	}

	@Test
	public void listTablesWithLimitOverTableCount() {
		String name = createTableName();
		createTable(name);
		ListTablesResult res = client.listTables(new ListTablesRequest().withLimit(5));
		Assert.assertTrue(res.getTableNames().contains(name));
		Assert.assertTrue(res.getTableNames().size() == 1);
	}

	@Test
	public void listTablesWithLimitUnderTableCount() {
		String name = createTableName();
		createTable();
		createTable(name);
		createTable();
		ListTablesResult res = client.listTables(new ListTablesRequest().withLimit(2));
		Assert.assertTrue(res.getTableNames().contains(name));
		Assert.assertTrue(res.getTableNames().size() == 2);
	}

	@Test
	public void listTablesWithExclusiveTableName() {
		String name = createTableName();
		createTable();
		createTable();
		createTable(name);
		ListTablesResult res = client.listTables(new ListTablesRequest().withExclusiveStartTableName(name));
		Assert.assertTrue(res.getTableNames().contains(name));
		Assert.assertTrue(res.getTableNames().size() == 1);
	}

	@Test
	public void listTablesWithLimitUnderTableCountAndExclusiveTableName() {
		String name = createTableName();
		createTable();
		createTable();
		createTable(name);
		createTable();
		createTable();
		ListTablesResult res = client.listTables(new ListTablesRequest().withLimit(1).withExclusiveStartTableName(name));
		Assert.assertTrue(res.getTableNames().contains(name));
		Assert.assertTrue(res.getTableNames().size() == 1);
	}

	@Test
	public void listTablesWithLimitOverTableCountAndExclusiveTableName() {
		String name = createTableName();
		createTable();
		createTable(name);
		createTable();
		createTable();
		createTable();
		ListTablesResult res = client.listTables(new ListTablesRequest().withLimit(10).withExclusiveStartTableName(name));
		Assert.assertTrue(res.getTableNames().contains(name));
		Assert.assertTrue(res.getTableNames().size() == 4);
	}

	@Test
	public void deleteTableTest() {
		String name = createTableName();
		createTable(name);
		client.deleteTable(new DeleteTableRequest(name));
		ListTablesResult res = client.listTables();
		Assert.assertFalse(res.getTableNames().contains(name));
		Assert.assertTrue(res.getTableNames().size() == 0);
	}

	@Test
	public void deleteTableWithoutName() {
		String name = createTableName();
		createTable(name);
		DeleteTableResult res = client.deleteTable(new DeleteTableRequest());
		Assert.assertNull(res.getTableDescription());
		Assert.assertTrue(client.listTables().getTableNames().contains(name));
	}

	@Test
	public void updateTable() {
		String name = createTableName();
		createTable(name);
		ProvisionedThroughput throughput = new ProvisionedThroughput().withReadCapacityUnits(50L).withWriteCapacityUnits(50L);
		UpdateTableRequest req = new UpdateTableRequest().withTableName(name).withProvisionedThroughput(throughput);
		Date date = new Date();
		TableDescription desc = client.updateTable(req).getTableDescription();
		Assert.assertNotNull(desc);
		Assert.assertEquals(name, desc.getTableName());
		Assert.assertEquals(Math.round(date.getTime() / 1000), Math.round(desc.getProvisionedThroughput().getLastDecreaseDateTime().getTime() / 1000));
		Assert.assertEquals(Math.round(date.getTime() / 1000), Math.round(desc.getProvisionedThroughput().getLastIncreaseDateTime().getTime() / 1000));
	}

	@Test
	public void updateTableWithoutName() {
		createTable();
		ProvisionedThroughput throughput = new ProvisionedThroughput().withReadCapacityUnits(50L).withWriteCapacityUnits(50L);
		UpdateTableRequest req = new UpdateTableRequest().withProvisionedThroughput(throughput);
		Assert.assertNull(client.updateTable(req).getTableDescription());
	}

	@Test
	public void updateTableWithoutThroughput() {
		String name = createTableName();
		createTable(name);
		UpdateTableRequest req = new UpdateTableRequest().withTableName(name);
		Assert.assertNull(client.updateTable(req).getTableDescription());
	}

	@Test
	public void updateTableWithoutNameOrThroughput() {
		createTable();
		Assert.assertNull(client.updateTable(new UpdateTableRequest()).getTableDescription());
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

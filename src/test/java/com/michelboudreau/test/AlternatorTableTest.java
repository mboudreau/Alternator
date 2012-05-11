package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodb.model.*;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternator.AlternatorDBClient;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.UUID;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext.xml"})
public class AlternatorTableTest {

	@Autowired
	private AlternatorDBClient client;
	@Autowired
	private DynamoDBMapper mapper;
	private AlternatorDB db;
	private String testTableName;

	@Before
	public void setUp() throws Exception {
		db = new AlternatorDB().start();
		client.setEndpoint("http://localhost:9090");
		testTableName = "Testing";
	}

	@After
	public void tearDown() throws Exception {
		this.db.stop();
	}


	@Test
	public void createTableWithHashKeySTest() {
		KeySchema schema = new KeySchema();
		schema.setHashKeyElement(getSSchema());
		CreateTableResult result = client.createTable(getRequest(schema));
		TableDescription desc = result.getTableDescription();
		Assert.assertNotNull(desc);
		Assert.assertEquals(desc.getKeySchema(), schema);
		Assert.assertEquals(desc.getTableName(), testTableName);
		testAfterCreatingTable();
	}

	@Test
	public void createTableWithHashKeyNTest() {
		KeySchema schema = new KeySchema();
		schema.setHashKeyElement(getNSchema());
		TableDescription res = client.createTable(getRequest(schema)).getTableDescription();
		Assert.assertNotNull(res);
		Assert.assertEquals(res.getKeySchema(), schema);
		Assert.assertEquals(res.getTableName(), testTableName);
		testAfterCreatingTable();
	}

	@Test
	public void createTableWithHashKeySRangeKeySTest() {
		KeySchema schema = new KeySchema();
		schema.setHashKeyElement(getSSchema());
		schema.setRangeKeyElement(getSSchema());
		TableDescription res = client.createTable(getRequest(schema)).getTableDescription();
		Assert.assertNotNull(res);
		Assert.assertEquals(res.getKeySchema(), schema);
		Assert.assertEquals(res.getTableName(), testTableName);
		testAfterCreatingTable();
	}


	@Test
	public void createTableWithHashKeySRangeKeyNTest() {
		KeySchema schema = new KeySchema();
		schema.setHashKeyElement(getSSchema());
		schema.setRangeKeyElement(getNSchema());
		TableDescription res = client.createTable(getRequest(schema)).getTableDescription();
		Assert.assertNotNull(res);
		Assert.assertEquals(res.getKeySchema(), schema);
		Assert.assertEquals(res.getTableName(), testTableName);
		testAfterCreatingTable();

	}

	@Test
	public void createTableWithHashKeyNRangeKeySTest() {
		KeySchema schema = new KeySchema();
		schema.setHashKeyElement(getNSchema());
		schema.setRangeKeyElement(getSSchema());
		TableDescription res = client.createTable(getRequest(schema)).getTableDescription();
		Assert.assertNotNull(res);
		Assert.assertEquals(res.getKeySchema(), schema);
		Assert.assertEquals(res.getTableName(), testTableName);
		testAfterCreatingTable();

	}

	@Test
	public void createTableWithHashKeyNRangeKeyNTest() {
		KeySchema schema = new KeySchema();
		schema.setHashKeyElement(getNSchema());
		schema.setRangeKeyElement(getNSchema());
		TableDescription res = client.createTable(getRequest(schema)).getTableDescription();
		Assert.assertNotNull(res);
		Assert.assertEquals(res.getKeySchema(), schema);
		Assert.assertEquals(res.getTableName(), testTableName);
		testAfterCreatingTable();
	}

	@Test
	public void createTableWithoutHashKey() {
		KeySchema schema = new KeySchema();
		schema.setRangeKeyElement(getNSchema());
		Assert.assertNull(client.createTable(getRequest(schema)).getTableDescription());
	}

	@Test
	public void createTableWithoutName() {
		KeySchema schema = new KeySchema();
		schema.setRangeKeyElement(getNSchema());
		CreateTableRequest req = new CreateTableRequest();
		req.setKeySchema(schema);
		req.setProvisionedThroughput(getThroughput());
		Assert.assertNull(client.createTable(req).getTableDescription());
	}

	@Test
	public void createTableWithoutThroughput() {
		KeySchema schema = new KeySchema();
		schema.setRangeKeyElement(getNSchema());
		CreateTableRequest req = new CreateTableRequest();
		req.setKeySchema(schema);
		req.setTableName(testTableName);
		Assert.assertNull(client.createTable(req).getTableDescription());
	}

	@Test
	public void deleteTableWithoutName() {
		KeySchema schema = new KeySchema();
		schema.setHashKeyElement(getSSchema());
		schema.setRangeKeyElement(getNSchema());
		client.createTable(new CreateTableRequest(testTableName, schema));
		DeleteTableRequest req = new DeleteTableRequest();
		client.deleteTable(req);
		Assert.assertTrue(client.listTables().getTableNames().contains(testTableName));
		deleteTableTest();
		Assert.assertFalse(client.listTables().getTableNames().contains(testTableName));
	}

	@Test
	public void updateTableWithoutName() {
		KeySchema schema = new KeySchema();
		schema.setHashKeyElement(getSSchema());
		schema.setRangeKeyElement(getNSchema());
		client.createTable(getRequest(schema));
		UpdateTableRequest up = new UpdateTableRequest();
		up.setProvisionedThroughput(getThroughput());
		UpdateTableResult res = client.updateTable(up);
		Assert.assertEquals(res.getTableDescription(), null);
	}

	@Test
	public void updateTableWithoutThroughput() {
		KeySchema schema = new KeySchema();
		schema.setHashKeyElement(getSSchema());
		schema.setRangeKeyElement(getNSchema());
		CreateTableRequest ct = new CreateTableRequest();
		ct.setKeySchema(schema);
		ct.setTableName(testTableName);
		Assert.assertNull(client.createTable(ct).getTableDescription());
	}

	@Test
	public void describeTableWithoutName() {
		DescribeTableResult res = client.describeTable(new DescribeTableRequest());
		Assert.assertEquals(res.getTable(), null);
	}

	protected KeySchemaElement getSSchema() {
		KeySchemaElement el = new KeySchemaElement();
		el.setAttributeName(UUID.randomUUID().toString().substring(1, 4));
		el.setAttributeType(ScalarAttributeType.S);
		return el;
	}

	protected KeySchemaElement getNSchema() {
		KeySchemaElement el = new KeySchemaElement();
		el.setAttributeName(UUID.randomUUID().toString().substring(1, 4));
		el.setAttributeType(ScalarAttributeType.N);
		return el;
	}

	protected ProvisionedThroughput getThroughput() {
		ProvisionedThroughput pt = new ProvisionedThroughput();
		pt.setReadCapacityUnits(5L);
		pt.setWriteCapacityUnits(5L);
		return pt;
	}

	protected CreateTableRequest getRequest(KeySchema schema) {
		CreateTableRequest request = new CreateTableRequest(testTableName, schema);
		request.setProvisionedThroughput(getThroughput());
		return request;
	}


	@Ignore
	@Test
	public void testAfterCreatingTable() {
		listTablesTest();
		describeTableTest();
		deleteTableTest();
	}

	@Ignore
	@Test
	public void describeTableTest() {
		DescribeTableRequest request = new DescribeTableRequest();
		request.setTableName(testTableName);
		Assert.assertEquals(client.describeTable(request).getTable().getTableName(), testTableName);
	}

	@Ignore
	@Test
	public void listTablesTest() {
		ListTablesResult res = client.listTables();
		Assert.assertTrue(res.getTableNames().contains(testTableName));
	}

	@Ignore
	@Test
	public void deleteTableTest() {
		DeleteTableRequest req = new DeleteTableRequest();
		req.setTableName(testTableName);
		client.deleteTable(req);
		Assert.assertFalse(client.listTables().getTableNames().contains(testTableName));
	}
}

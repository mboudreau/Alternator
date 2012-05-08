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
		TableDescription res = client.createTable(new CreateTableRequest(testTableName, schema)).getTableDescription();
        Assert.assertNotNull(res);
        Assert.assertEquals(res.getKeySchema(),schema);
        Assert.assertEquals(res.getTableName(), testTableName);
        testAfterCreatingTable();
    }

    @Test
    public void createTableWithHashKeyNTest(){
        KeySchema schema = new KeySchema();
        schema.setHashKeyElement(getNSchema());
        TableDescription res = client.createTable(new CreateTableRequest(testTableName, schema)).getTableDescription();
        Assert.assertNotNull(res);
        Assert.assertEquals(res.getKeySchema(),schema);
        Assert.assertEquals(res.getTableName(),testTableName);
        testAfterCreatingTable();
    }

    @Test
    public void createTableWithHashKeySRangeKeySTest() {
        KeySchema schema = new KeySchema();
        schema.setHashKeyElement(getSSchema());
        schema.setRangeKeyElement(getSSchema());
        TableDescription res = client.createTable(new CreateTableRequest(testTableName, schema)).getTableDescription();
        Assert.assertNotNull(res);
        Assert.assertEquals(res.getKeySchema(),schema);
        Assert.assertEquals(res.getTableName(),testTableName);
        testAfterCreatingTable();
    }



    @Test
    public void createTableWithHashKeySRangeKeyNTest() {
        KeySchema schema = new KeySchema();
        schema.setHashKeyElement(getSSchema());
        schema.setRangeKeyElement(getNSchema());
        TableDescription res = client.createTable(new CreateTableRequest(testTableName, schema)).getTableDescription();
        Assert.assertNotNull(res);
        Assert.assertEquals(res.getKeySchema(),schema);
        Assert.assertEquals(res.getTableName(),testTableName);
        testAfterCreatingTable();

    }

    @Test
    public void createTableWithHashKeyNRangeKeySTest() {
        KeySchema schema = new KeySchema();
        schema.setHashKeyElement(getNSchema());
        schema.setRangeKeyElement(getSSchema());
        TableDescription res = client.createTable(new CreateTableRequest(testTableName, schema)).getTableDescription();
        Assert.assertNotNull(res);
        Assert.assertEquals(res.getKeySchema(),schema);
        Assert.assertEquals(res.getTableName(),testTableName);
        testAfterCreatingTable();

    }

    @Test
    public void createTableWithHashKeyNRangeKeyNTest() {
        KeySchema schema = new KeySchema();
        schema.setHashKeyElement(getNSchema());
        schema.setRangeKeyElement(getNSchema());
        TableDescription res = client.createTable(new CreateTableRequest(testTableName, schema)).getTableDescription();
        Assert.assertNotNull(res);
        Assert.assertEquals(res.getKeySchema(), schema);
        Assert.assertEquals(res.getTableName(), testTableName);
        testAfterCreatingTable();
    }

    public KeySchemaElement getSSchema(){
        KeySchemaElement el = new KeySchemaElement();
        el.setAttributeName(UUID.randomUUID().toString().substring(1,4));
        el.setAttributeType(ScalarAttributeType.S);
        return el;
    }

    public KeySchemaElement getNSchema(){
        KeySchemaElement el = new KeySchemaElement();
        el.setAttributeName(UUID.randomUUID().toString().substring(1,4));
        el.setAttributeType(ScalarAttributeType.N);
        return el;
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
    public void describeTableTest(){
        DescribeTableRequest request = new DescribeTableRequest();
        request.setTableName(testTableName);
        Assert.assertEquals(client.describeTable(request).getTable().getTableName(),testTableName);
    }

    @Ignore
    @Test
    public void listTablesTest(){
        ListTablesResult res = client.listTables();
        Assert.assertTrue(res.getTableNames().contains(testTableName));
    }

    @Ignore
    @Test
    public void deleteTableTest(){
        DeleteTableRequest req = new DeleteTableRequest();
        req.setTableName(testTableName);
        client.deleteTable(req);
        Assert.assertFalse(client.listTables().getTableNames().contains(testTableName));
    }
}

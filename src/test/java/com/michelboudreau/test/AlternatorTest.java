package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ScalarAttributeType;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternator.AlternatorDBClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext.xml"})
public class AlternatorTest {

	@Autowired
	private AlternatorDBClient client;
	@Autowired
	private DynamoDBMapper mapper;
	private AlternatorDB db;

	@Before
	public void setUp() throws Exception {
		db = new AlternatorDB().start();
		client.setEndpoint("http://localhost:9090");
	}

	@After
	public void tearDown() throws Exception {
		this.db.stop();
	}

	@Test
	public void createTableTest() {
		KeySchema schema = new KeySchema();
		KeySchemaElement el = new KeySchemaElement();
		el.setAttributeName("id");
		el.setAttributeType(ScalarAttributeType.S);
		schema.setHashKeyElement(el);
		client.createTable(new CreateTableRequest("Testing", schema));
	}
}

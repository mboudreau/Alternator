package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternator.AlternatorDBClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.springframework.beans.factory.annotation.Autowired;

public class AlternatorTest {
	static protected AlternatorDBClient client;
	static protected DynamoDBMapper mapper;
	static protected AlternatorDB db;

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
}

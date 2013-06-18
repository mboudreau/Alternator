package com.michelboudreau.testv2;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AlternatorTableTest.class, AlternatorItemTest.class, AlternatorQueryTest.class, AlternatorScanTest.class, AlternatorBatchItemTest.class,
		AlternatorMapperTest.class })
public class AlternatorInProcessTestSuite {

	@BeforeClass
	public static void before() {
		AlternatorTest.RUN_DB_AS_SERVICE = false;
	}
}

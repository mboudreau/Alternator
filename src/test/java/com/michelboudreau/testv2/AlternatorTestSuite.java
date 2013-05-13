package com.michelboudreau.testv2;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AlternatorTableTest.class , AlternatorItemTest.class , AlternatorQueryTest.class, AlternatorScanTest.class, AlternatorBatchItemTest.class, AlternatorSaveRestoreTest.class, AlternatorMapperTest.class})
public class AlternatorTestSuite {
	@BeforeClass
	public static void before() {
		AlternatorTest.RUN_DB_AS_SERVICE = true;
	}
}

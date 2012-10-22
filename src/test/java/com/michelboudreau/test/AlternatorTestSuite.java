package com.michelboudreau.test;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AlternatorTableTest.class , AlternatorItemTest.class , AlternatorQueryTest.class, AlternatorScanTest.class, AlternatorBatchItemTest.class})
public class AlternatorTestSuite {
}

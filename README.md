Alternator - A DynamoDB Mock Server
==========

THIS IS A WORK IN PROGRESS - ALPHA VERSION - SOME THINGS ARE NOT 100%

While working on a new startup called [Tivity](http://www.tivity.us), my co-worker, [Thomas](https://github.com/tnbredillet), and I found that testing simple functions with DynamoDB was a pain because it would take time to create testing tables and tear them back down.  This would slow down our BDD approach to this project.  Another nuisance was the cost that would incur when using DynamoDB from our local webapp or that we would hit our throughput limit and cause an error.  Of course, this also means that you couldn't code things on the road because you needed an internet connect for Amazon Web Services.

To fix this, we've created a local data-store used for testing and development purposes by replicating DynamoDB's functionality.  Our goal is to simplify the development and testing of DynamoDB specific functionality.

You can now add the dependency into your maven project with:

	<dependencies>
		<dependency>
			<groupId>com.michelboudreau</groupId>
			<artifactId>alternator</artifactId>
			<version>0.3.4 <!-- subject to change, check sonatype repo --></version>
			<scope>test</scope>
		</dependency>
	</dependencies>
	<repositories>
		<repository>
			<snapshots>
				<enabled>true</enabled>
			</snapshots>
			<releases>
				<enabled>true</enabled>
			</releases>
			<id>sonatype-nexus</id>
			<url>https://oss.sonatype.org/content/groups/public</url>
		</repository>
	</repositories>

To get started, you need a 2 things: AlternatorDB (the service) and AlternatorDBClient (The client that calls the service).  You can use DynamoDBMapper as long as the AlternatorDBClient is specified within it.  The AlternatorDBClient is needed to circumvent the authentication process to AWS.

As a very simple example, you could do something like this in a test class:

	public class AlternatorTest {

		private AlternatorDBClient client;
		private DynamoDBMapper mapper;
		private AlternatorDB db;

		@Before
		public void setUp() throws Exception {
			this.client = new AlternatorDBClient();
			this.mapper = new DynamoDBMapper(this.client);
			this.db = new AlternatorDB().start();
		}

		@After
		public void tearDown() throws Exception {
			this.db.stop();
		}

		@Test
		public void test() {
			// Add test here that uses the mapper or the client
		}
	}

The AlternatorDB service defaults to port 9090, but can be changed in the constructor.  If you change the port for the service, you also need to change the port to the client by using the setEndpoint function.

Of course, it's not always possible to just create the client, mapper and service within the test.  If you're using the mapper and client in other parts of the system, you need a way to test that piece of code as well while replacing the client with the new one.  If you're injecting the client within your code, be sure to inject the interface `AmazonDynamoDB` and not the implementation `AmazonDynamoDBClient` to leave the client interchangeable.  You then simply need to use the test context, here's ours:

#### testContext.xml
	<?xml version="1.0" encoding="UTF-8"?>
	<beans xmlns="http://www.springframework.org/schema/beans"
		   xmlns:context="http://www.springframework.org/schema/context"
		   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
		   xmlns:beans="http://www.springframework.org/schema/beans"
		   xsi:schemaLocation="http://www.springframework.org/schema/context
				http://www.springframework.org/schema/context/spring-context-3.1.xsd
				http://www.springframework.org/schema/beans
				http://www.springframework.org/schema/beans/spring-beans-3.1.xsd">
		<bean id="dynamoDBClient" class="com.michelboudreau.alternator.AlternatorDBClient"/>
		<bean id="dynamoDBMapper" class="com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper">
			<constructor-arg index="0" ref="dynamoDBClient"/>
		</bean>
	</beans>

Then you only need to create the AlternatorDB service in your test and you're ready to test out your code.  Hope this helps out.  Please feel free to contribute or suggest ways to make the project better.


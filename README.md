Alternator - A DynamoDB Mock Server
==========

THIS IS A WORK IN PROGRESS - ALPHA VERSION

While working on a new startup called [Tivity](http://www.tivity.us), my co-worker, [Thomas](https://github.com/tnbredillet), and I found that testing simple functions with DynamoDB was a pain because it would take time to create testing tables and tear them back down.  This would slow down our BDD approach to this project.  Another thing that was annoying is that since we didn't want to incur costs when testing locally, so it would happen quite often that we would max our throughput and an error would occur.  Of course, this also means that you couldn't code things on the road because you needed an internet connect for the Amazon services.

Simply put, it's a local DynamoDB instance used for testing and development purposes.  Our goal is to make something quick and simple that imitates all of DynamoDB's functionality.  To get started, you just need to start the Alternator service using jetty by doing:

    mvn jetty:run

Afterwards, you only need to set the endpoint of your DynamoDBClient in your test, or in your config if you want to do local development.

#### Within Tests
    @Autowired
	private AmazonDynamoDBClient client;

	@Before
	public void setUp() {
		client.setEndpoint("http://localhost:9090");
	}

#### Within Config (applicationContext.xml)
    <bean id="dynamoDBClient" class="com.amazonaws.services.dynamodb.AmazonDynamoDBClient">
        <constructor-arg index="0" ref="awsCreds"/>
        <property name="endpoint" value="http://localhost:9090"/>
    </bean>

That's it.  Please feel free to contribute on the project.
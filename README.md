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
			<version>0.3.3 <!-- subject to change, check sonatype repo --></version>
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

## Building and Running Alternator as a Standalone Executable JAR File

An optional Maven profile named **"exejar"** includes all third-party dependencies into a self-contained executable JAR file. This allows the Alternator emulator to run in its own process. This is particularly useful when developing DynamoDB client applications in technologies other than Java.

### Building the Executable JAR File

Clone the Alternator GitHub repository to your local workstation and run the following Maven command:

    git clone https://github.com/mboudreau/Alternator.git
    cd Alternator
    mvn clean install -DskipTests -Pexejar
  
The desired executable JAR file will be present in the ./target sub-folder,
as well as in your local Maven repository in this folder tree:

    Windows:

      %USERPROFILE%\.m2\repository\com\michelboudreau\alternator\
      
    Linux or MacOSX:
    
      ~/.m2/repository/com/michelboudreau/alternator/
    
Note that none of the files in this folder need to be deployed to a server.
These files are intended for local use on a developer workstation.

### Starting and Stopping the Executable JAR File

To start the Alternator emulator process, use the following commands in a command prompt or terminal window (or save them in *.bat or *.sh script):

    Windows:
    
      set ALTERNATOR_VERSION=0.3.4-SNAPSHOT
      set ALTERNATOR_HOME=%USERPROFILE%\.m2\repository\com\michelboudreau\alternator\%ALTERNATOR_VERSION%
      java -jar "%ALTERNATOR_HOME%\alternator-%ALTERNATOR_VERSION%-jar-with-dependencies.jar" Alternator.db
      
    Linux or MacOSX:
    
      ALTERNATOR_VERSION=0.3.4-SNAPSHOT
      ALTERNATOR_HOME=~/.m2/repository/com/michelboudreau/alternator/${ALTERNATOR_VERSION}
      java -jar "${ALTERNATOR_HOME}/alternator-${ALTERNATOR_VERSION}-jar-with-dependencies.jar" Alternator.db

Note the following message that appears after the program initializes:

    Press the Enter key to exit gracefully and save data to file: 
    (folder-name)\Alternator.db
    Use Control-C to force exit (without saving data changes):

If you press Enter, the emulator stops and saves its memory database to the indicated file,
which will be reloaded the next time you run the emulator with the same filename argument.

If you press Control-C, the process is "hard-killed" and the memory data is discarded.

If you elect to save the data to **Alternator.db**, you can examine the JSON text with any text editor (set to word wrap).
The next time the emulator is started using the executable JAR file it will reload the saved data into memory.

To start an emulator session with an empty database, simply delete the Alternator.db file (or rename it out of the way).

### Accessing the Alternator Process from non-Java Applications

#### _.NET Example_:

Use the **The Amazon Web Services SDK for .NET** and include a reference in your Visual Studio project.

Here is an example class to obtain an **AmazonDynamoDB** client reference pointing to the emulator process rather than an actual DynamoDB server. Note that live Amazon credentials are needed for the initial constructor; however, these credentials are _not_ used for the remainder of the usage of the emulator client.

    using System;
    using System.Configuration;
    using Amazon.DynamoDB;
    using Amazon.SecurityToken;
    using Amazon.Runtime;

    namespace CSharpDynamoDbApplication
    {
      public class AlternatorExample
      {
        public const String DefaultAlternatorEndpoint = "http://localhost:9090/";

        public AmazonDynamoDB GetEmulatorClient()
        {
          // Divert the web service calls from the real DynamoDB to the Alternator mock server,
          // which must be running in a separate process via the executable JAR file.
          var config = new AmazonDynamoDBConfig
          {
            ServiceURL = DefaultAlternatorEndpoint
          };

          var accessKey = ConfigurationManager.AppSettings["AWSAccessKey"];
          var secretKey = ConfigurationManager.AppSettings["AWSSecretKey"];

          var stsClient = new AmazonSecurityTokenServiceClient(accessKey, secretKey);
          var sessionCredentials = new RefreshingSessionAWSCredentials(stsClient);

          // NOTE: This will throw an Amazon.SecurityToken.AmazonSecurityTokenServiceException
          //       if our accessKey and/or secretKey are invalid.
          var credentials = sessionCredentials.GetCredentials();

          var mockClient = new AmazonDynamoDBClient(sessionCredentials, config);

          return mockClient;
        }
      }
    }

#### _Node.js Example_:

Use NPM to install the **aws-sdk** package version **"0.9.1-pre.2"**.

Here is an example Node JavaScript module to obtain an **AmazonDynamoDB** client reference pointing to the emulator process rather than an actual DynamoDB server. Note that live Amazon credentials are _**not**_ needed for the Node.js client when working with the Alternator emulator process.


    // AWS client home: http://aws.amazon.com/sdkfornodejs/
    // API doc: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/frames.html
    var awssdk = require('aws-sdk');

    var awsDbClient = null;

    // Configure this module prior to use.
    //   settings (required) A settings object with a .dynamodb
    //           sub-object with appropriate configuration properties.
    //   callback = function(err, endpoint)
    //       err returns null on success, or an error message on failure.
    //       endpoint returns null on failure, 
    //       or a string indicating the active DynamoDB endpoint on success.
    exports.configure = 
    function (settings, callback) {
        var credentialsPath = settings.dynamodb.awsCredentialsPath;
        awssdk.config.loadFromPath(credentialsPath);

        var options = {};
        if (settings.dynamodb.useEmulator) {
            options.endpoint = settings.dynamodb.emulatorEndpoint;
        }

        var service = new awssdk.DynamoDB(options);
        awsDbClient = service.client;

        var endpoint = awsDbClient.config.endpoint;
        if (!endpoint) {
            endpoint = awsDbClient.config.region;
        }
        
        callback(null, endpoint);
    };


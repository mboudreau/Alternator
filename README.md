Alternator - A DynamoDB Mock Server
==========

( Works with V1 and V2 of DynamoDB )

THIS IS A WORK IN PROGRESS - BETA VERSION - SOME THINGS ARE NOT 100%

While working on a new startup called [Tivity](http://www.tivity.us), my co-worker, [Thomas](https://github.com/tnbredillet), and I found that testing simple functions with DynamoDB was a pain because it would take time to create testing tables and tear them back down.  This would slow down our BDD approach to this project.  Another nuisance was the cost that would incur when using DynamoDB from our local webapp or that we would hit our throughput limit and cause an error.  Of course, this also means that you couldn't code things on the road because you needed an internet connect for Amazon Web Services.

To fix this, we've created a local data-store used for testing and development purposes by replicating DynamoDB's functionality.  Our goal is to simplify the development and testing of DynamoDB specific functionality.

You can now add the dependency into your maven project with:

	<dependencies>
		<dependency>
			<groupId>com.michelboudreau</groupId>
			<artifactId>alternator</artifactId>
			<version>0.6.0 <!-- subject to change, check sonatype repo --></version>
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

       import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;	
       import com.michelboudreau.alternator.AlternatorDB;
       import com.michelboudreau.alternator.AlternatorDBClient;

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

#### Testing applicationContext.xml
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

## Dual API Version Support

Amazon created a new specification for the DynamoDB API that is _not_ backward compatible with the earlier version.
The earlier version of the API is still supported but is _deprecated_.

The Alternator emulator allows both versions of the DynamoDB API.

**Note** however that only the features that were already available thru the original API version are supported.  
When processing against the newer API protocol, Alternator simply maps the request objects to the original foramt and calls the pre-existing logic.
It then maps the result to the new API protocol format.
Any exceptions are also remapped from the **dynamodb** namespace to the **dynamodbv2** namespace.

### Java API Documentation

The original API was available for Java via the 1.3.33 version of the **com.amazonaws aws-java-sdk** maven package.
It is still supported by the 1.4.2 version of the Maven package.

The Java Doc pages for both API versions reside here:

**<http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html>**

Focus on the **Package com.amazonaws.services.dynamodb** package namespace tree for the original API methods.

Focus on the **Package com.amazonaws.services.dynamodbv2** package namespace tree for the new API methods.

### Node.js API Documentation

The overall documentation for the Node.js AWS-SDK resides here:

**<http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/frames.html>**

The section for **DynamoDB** has two sub-sections.

The original API documentation is at <http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB_20111205.html>

The new API documentation is at <http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB_20120810.html>

The Node.js NPM package for the AWS-SDK client support either API version can be obtained with this entry in your **package.json**

    "dependencies": {
        "aws-sdk": "1.0.0"
    },

By default this client will use the newer '2012-08-10' API protocol, corresponding to the **com.amazonaws.services.dynamodbv2** namespaces.
You can revert to the earlier '2011-12-05' API protocol by using an optional parameter in the constructor:

    var dynamodb = new AWS.DynamoDB({apiVersion: '2011-12-05'});

The earlier **0.9.-pre.#** versions of the NPM package always assume the original '2011-12-05' API protocol.

    "dependencies": {
        "aws-sdk": "0.9.2-pre.3"

    },

#### Revised Unit Test Example

You can test Java code that uses the revised API protocol by importing from the **v2** package namespaces:

       import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;	
       import com.michelboudreau.alternator.AlternatorDB;
       import com.michelboudreau.alternatorv2.AlternatorDBClientV2;

       public class AlternatorTest {

		private AlternatorDBClientV2 client;
		private DynamoDBMapper mapper;
		private AlternatorDB db;

		@Before
		public void setUp() throws Exception {
			this.client = new AlternatorDBClientV2();
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

#### Revised Testing applicationContext.xml

For unit tests using the new "v2" client, here is the **applicationContext.xml** file:

    <?xml version="1.0" encoding="UTF-8"?>
    <beans xmlns="http://www.springframework.org/schema/beans"
           xmlns:context="http://www.springframework.org/schema/context"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xmlns:beans="http://www.springframework.org/schema/beans"
           xsi:schemaLocation="http://www.springframework.org/schema/context
                http://www.springframework.org/schema/context/spring-context-3.1.xsd
                http://www.springframework.org/schema/beans
                http://www.springframework.org/schema/beans/spring-beans-3.1.xsd">
      <!-- Turn on post-processing (Exception translation, etc) -->
      <context:annotation-config/>

      <bean id="dynamoDBClient" class="com.michelboudreau.alternator.AlternatorDBClient"/>
      <bean id="dynamoDBMapper" class="com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper">
        <constructor-arg index="0" ref="dynamoDBClient"/>
      </bean>

      <bean id="dynamoDBClientV2" class="com.michelboudreau.alternatorv2.AlternatorDBClientV2"/>
      <bean id="dynamoDBMapperV2" class="com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper">
        <constructor-arg index="0" ref="dynamoDBClientV2"/>
      </bean>
    </beans>


## Building and Running Alternator as a Standalone Executable JAR File

An optional Maven profile named **"standalone"** includes all third-party dependencies into a self-contained executable JAR file. This allows the Alternator emulator to run in its own process. This is particularly useful when developing DynamoDB client applications in technologies other than Java.

**_Important Caveat:_**
Currently the Alternator emulator is **_not_** thread-safe for insertions. The internal memory structures are not protected by synchronization locks.  Your own application should serialize its insert operations so they occur one at a time.

### Building the Executable JAR File

The standalone executable JAR file is _excluded_ from the default Maven profile **install** target since the file is about 16 MB due to inclusion of the required 3rd-party dependency libraries.  To obtain a copy of this JAR file, clone the Alternator GitHub repository to your local workstation and run the following Maven command:

    git clone https://github.com/mboudreau/Alternator.git
    cd Alternator
    mvn clean install -DskipTests -Pstandalone
  
The desired executable JAR file will be present in the ./target sub-folder,
as well as in your local Maven repository in this folder tree:

    Windows:

      %USERPROFILE%\.m2\repository\com\michelboudreau\alternator\
      
    Linux or MacOSX:
    
      ~/.m2/repository/com/michelboudreau/alternator/
    
Note that none of the files in this folder need to be deployed to a server.
These files are intended for local use on a developer workstation.

### Starting and Stopping the Executable JAR File

To start the Alternator emulator process, use the following commands in a command prompt or terminal window:

    Windows:
    
      set ALTERNATOR_VERSION=0.5.1-SNAPSHOT
      set ALTERNATOR_HOME=%USERPROFILE%\.m2\repository\com\michelboudreau\alternator\%ALTERNATOR_VERSION%
      java -jar "%ALTERNATOR_HOME%\alternator-%ALTERNATOR_VERSION%-jar-with-dependencies.jar" Alternator.db
      
    Linux or MacOSX:
    
      ALTERNATOR_VERSION=0.5.1-SNAPSHOT
      ALTERNATOR_HOME=~/.m2/repository/com/michelboudreau/alternator/${ALTERNATOR_VERSION}
      java -jar "${ALTERNATOR_HOME}/alternator-${ALTERNATOR_VERSION}-jar-with-dependencies.jar" Alternator.db

These command sequences are available in a **.bat** (for Windows) and **.sh** (Linux or MacOSX) script in the **scripts** folder beneath the root of this Git repository.
      
Note the following message that appears after the program initializes:

    Press the Enter key to exit gracefully and save data to file: 
    (folder-name)\Alternator.db
    Use Control-C to force exit (without saving data changes):

If you press Enter, the emulator stops and saves its memory database to the indicated file,
which will be reloaded the next time you run the emulator with the same filename argument.

If you press Control-C, the process is "hard-killed" and the memory data is discarded.

If you elect to save the data to **Alternator.db**, you can examine the JSON text with any text editor (set to word wrap).
The next time the emulator is started using the executable JAR file it will reload the saved data into memory.

To start an emulator session with an empty database, simply delete the **Alternator.db** file (or rename it out of the way).  You can also save copies of the **Alternator.db** file for use in repeatable integration testing for your application.

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

          var mockClient = new AmazonDynamoDBClient(accessKey, secretKey, config);

          return mockClient;
        }
      }
    }

#### _Node.js Example_:

Use NPM to install the **aws-sdk** package version **"0.9.1-pre.2"** (or higher).

Here is an example Node JavaScript module to obtain an **AmazonDynamoDB** client reference pointing to the emulator process rather than an actual DynamoDB server. Note that live Amazon credentials are _**not**_ needed for the Node.js client when working with the Alternator emulator process.

    // AWS client home: http://aws.amazon.com/sdkfornodejs/
    // API doc: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/frames.html
    var awssdk = require('aws-sdk');

    var awsDbClient = null;
    exports.awsDbClient = awsDbClient;

    // Configure this module prior to use.
    //   settings (required) A settings object with a .dynamodb
    //           sub-object with appropriate configuration properties.
    //   callback = function(err, endpoint)
    //       err returns null on success, or an error message on failure.
    //       endpoint returns null on failure, 
    //       or a string indicating the active DynamoDB endpoint on success.
    exports.configure = 
    function (settings, callback) {
        var defaultAlternatorEndpoint = "http://localhost:9090/";

        var credentialsPath = settings.dynamodb.awsCredentialsPath;
        awssdk.config.loadFromPath(credentialsPath);

        var options = {};
        if (settings.dynamodb.useEmulator) {
            options.endpoint = defaultAlternatorEndpoint;
        }

        var service = new awssdk.DynamoDB(options);
        awsDbClient = service.client;

        var endpoint = awsDbClient.config.endpoint;
        if (!endpoint) {
            endpoint = awsDbClient.config.region;
        }
        
        callback(null, endpoint);
    };

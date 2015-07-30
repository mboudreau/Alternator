package com.michelboudreau.alternatorv2;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResponse;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.handlers.HandlerChainFactory;
import com.amazonaws.http.ExecutionContext;
import com.amazonaws.http.HttpResponseHandler;
import com.amazonaws.http.JsonErrorResponseHandler;
import com.amazonaws.http.JsonResponseHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.model.transform.BatchGetItemRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.BatchGetItemResultJsonUnmarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.BatchWriteItemRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.BatchWriteItemResultJsonUnmarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.CreateTableRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.CreateTableResultJsonUnmarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.DeleteItemRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.DeleteItemResultJsonUnmarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.DeleteTableRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.DeleteTableResultJsonUnmarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.DescribeTableRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.DescribeTableResultJsonUnmarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.GetItemRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.GetItemResultJsonUnmarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.ListTablesRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.ListTablesResultJsonUnmarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.PutItemRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.PutItemResultJsonUnmarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.QueryRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.QueryResultJsonUnmarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.ScanRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.ScanResultJsonUnmarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.UpdateItemRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.UpdateItemResultJsonUnmarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.UpdateTableRequestMarshaller;
import com.amazonaws.services.dynamodbv2.model.transform.UpdateTableResultJsonUnmarshaller;
import com.amazonaws.transform.JsonErrorUnmarshaller;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.Unmarshaller;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public class AlternatorDBClientV2 extends AmazonWebServiceClient implements AmazonDynamoDB {
	private static final Log log = LogFactory.getLog(AlternatorDBClientV2.class);
	protected List<JsonErrorUnmarshaller> exceptionUnmarshallers;

	public AlternatorDBClientV2() {
		this(new ClientConfiguration());
	}

	public AlternatorDBClientV2(ClientConfiguration clientConfiguration) {
		super(clientConfiguration);
		init();
	}

	private void init() {
		exceptionUnmarshallers = new ArrayList<JsonErrorUnmarshaller>();

		exceptionUnmarshallers.add(new JsonErrorUnmarshaller());
		setEndpoint("http://localhost:9090/");

		HandlerChainFactory chainFactory = new HandlerChainFactory();
		requestHandler2s.addAll(chainFactory.newRequestHandlerChain("/com/amazonaws/services/dynamodb/request.handlers"));

		ClientConfiguration customClientConfiguration = new ClientConfiguration(clientConfiguration);
                customClientConfiguration.setSignerOverride("NoOpSignerType");
		if (customClientConfiguration.getRetryPolicy() == ClientConfiguration.DEFAULT_RETRY_POLICY) {
			log.debug("Overriding default max error retry value to: " + 10);
			customClientConfiguration.setMaxErrorRetry(10);
		}
		setCustomConfiguration(customClientConfiguration);
	}

        private void setCustomConfiguration(ClientConfiguration customClientConfiguration) {
            this.clientConfiguration = customClientConfiguration;
        }

    @Override
	public ListTablesResult listTables(ListTablesRequest listTablesRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<ListTablesRequest> request = new ListTablesRequestMarshaller().marshall(listTablesRequest);

		Unmarshaller<ListTablesResult, JsonUnmarshallerContext> unmarshaller = new ListTablesResultJsonUnmarshaller();
		JsonResponseHandler<ListTablesResult> responseHandler = new JsonResponseHandler<ListTablesResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

    @Override
	public QueryResult query(QueryRequest queryRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<QueryRequest> request = new QueryRequestMarshaller().marshall(queryRequest);

		Unmarshaller<QueryResult, JsonUnmarshallerContext> unmarshaller = new QueryResultJsonUnmarshaller();
		JsonResponseHandler<QueryResult> responseHandler = new JsonResponseHandler<QueryResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

    @Override
	public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<BatchWriteItemRequest> request = new BatchWriteItemRequestMarshaller().marshall(batchWriteItemRequest);

		Unmarshaller<BatchWriteItemResult, JsonUnmarshallerContext> unmarshaller = new BatchWriteItemResultJsonUnmarshaller();
		JsonResponseHandler<BatchWriteItemResult> responseHandler = new JsonResponseHandler<BatchWriteItemResult>(unmarshaller);

		return invoke(request, responseHandler);
    }

    @Override
        public BatchWriteItemResult batchWriteItem(java.util.Map<String,java.util.List<WriteRequest>> requestItems)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("batchWriteItem using Map is not implemented in Alternator.");
        }

    @Override
	public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<UpdateItemRequest> request = new UpdateItemRequestMarshaller().marshall(updateItemRequest);

		Unmarshaller<UpdateItemResult, JsonUnmarshallerContext> unmarshaller = new UpdateItemResultJsonUnmarshaller();
		JsonResponseHandler<UpdateItemResult> responseHandler = new JsonResponseHandler<UpdateItemResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

    @Override
        public UpdateItemResult updateItem(String tableName, java.util.Map<String, AttributeValue> key, java.util.Map<String, AttributeValueUpdate> attributeUpdates, String returnValues)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("updateItem using String, Map, Map, and String is not implemented in Alternator.");
        }

    @Override
        public UpdateItemResult updateItem(String tableName, java.util.Map<String,AttributeValue> key, java.util.Map<String,AttributeValueUpdate> attributeUpdates)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("updateItem using String, Map, and Map is not implemented in Alternator.");
        }

    @Override
	public PutItemResult putItem(PutItemRequest putItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<PutItemRequest> request = new PutItemRequestMarshaller().marshall(putItemRequest);

		Unmarshaller<PutItemResult, JsonUnmarshallerContext> unmarshaller = new PutItemResultJsonUnmarshaller();
		JsonResponseHandler<PutItemResult> responseHandler = new JsonResponseHandler<PutItemResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

    @Override
        public PutItemResult putItem(String tableName, java.util.Map<String,AttributeValue> item, String returnValues)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("putItem using String, Map, and String is not implemented in Alternator.");
        }

    @Override
        public PutItemResult putItem(String tableName, java.util.Map<String,AttributeValue> item)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("putItem using String and Map is not implemented in Alternator.");
        }

    @Override
	public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<DescribeTableRequest> request = new DescribeTableRequestMarshaller().marshall(describeTableRequest);

		Unmarshaller<DescribeTableResult, JsonUnmarshallerContext> unmarshaller = new DescribeTableResultJsonUnmarshaller();
		JsonResponseHandler<DescribeTableResult> responseHandler = new JsonResponseHandler<DescribeTableResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

    @Override
        public DescribeTableResult describeTable(String tableName)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("describeTable using String is not implemented in Alternator.");
        }

    @Override
	public ScanResult scan(ScanRequest scanRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<ScanRequest> request = new ScanRequestMarshaller().marshall(scanRequest);

		Unmarshaller<ScanResult, JsonUnmarshallerContext> unmarshaller = new ScanResultJsonUnmarshaller();
		JsonResponseHandler<ScanResult> responseHandler = new JsonResponseHandler<ScanResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

    @Override
        public ScanResult scan(String tableName, java.util.List<String> attributesToGet, java.util.Map<String,Condition> scanFilter)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("scan using String, List, and Map is not implemented in Alternator.");
        }

    @Override
        public ScanResult scan(String tableName, java.util.Map<String,Condition> scanFilter)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("scan using String and Map is not implemented in Alternator.");
        }

    @Override
        public ScanResult scan(String tableName, java.util.List<String> attributesToGet)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("scan using String and List is not implemented in Alternator.");
        }

    @Override
	public CreateTableResult createTable(CreateTableRequest createTableRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<CreateTableRequest> request = new CreateTableRequestMarshaller().marshall(createTableRequest);

		Unmarshaller<CreateTableResult, JsonUnmarshallerContext> unmarshaller = new CreateTableResultJsonUnmarshaller();
		JsonResponseHandler<CreateTableResult> responseHandler = new JsonResponseHandler<CreateTableResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

    @Override
        public CreateTableResult createTable(java.util.List<AttributeDefinition> attributeDefinitions, String tableName, java.util.List<KeySchemaElement> keySchema, ProvisionedThroughput provisionedThroughput)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("createTable using List, String, List, and ProvisionedThroughput is not implemented in Alternator.");
        }

    @Override
	public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<UpdateTableRequest> request = new UpdateTableRequestMarshaller().marshall(updateTableRequest);

		Unmarshaller<UpdateTableResult, JsonUnmarshallerContext> unmarshaller = new UpdateTableResultJsonUnmarshaller();
		JsonResponseHandler<UpdateTableResult> responseHandler = new JsonResponseHandler<UpdateTableResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

    @Override
        public UpdateTableResult updateTable(String tableName, ProvisionedThroughput provisionedThroughput)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("updateTable using String and ProvisionedThroughput is not implemented in Alternator.");
        }

    @Override
	public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<DeleteTableRequest> request = new DeleteTableRequestMarshaller().marshall(deleteTableRequest);

		Unmarshaller<DeleteTableResult, JsonUnmarshallerContext> unmarshaller = new DeleteTableResultJsonUnmarshaller();
		JsonResponseHandler<DeleteTableResult> responseHandler = new JsonResponseHandler<DeleteTableResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

    @Override
        public DeleteTableResult deleteTable(String tableName)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("deleteTable using String is not implemented in Alternator.");
        }

    @Override
	public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<DeleteItemRequest> request = new DeleteItemRequestMarshaller().marshall(deleteItemRequest);

		Unmarshaller<DeleteItemResult, JsonUnmarshallerContext> unmarshaller = new DeleteItemResultJsonUnmarshaller();
		JsonResponseHandler<DeleteItemResult> responseHandler = new JsonResponseHandler<DeleteItemResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

    @Override
        public DeleteItemResult deleteItem(String tableName, java.util.Map<String,AttributeValue> key, String returnValues)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("deleteTable using String, Map, and String is not implemented in Alternator.");
        }

    @Override
        public DeleteItemResult deleteItem(String tableName, java.util.Map<String,AttributeValue> key)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("deleteItem using String and Map is not implemented in Alternator.");
        }

    @Override
	public GetItemResult getItem(GetItemRequest getItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<GetItemRequest> request = new GetItemRequestMarshaller().marshall(getItemRequest);

		Unmarshaller<GetItemResult, JsonUnmarshallerContext> unmarshaller = new GetItemResultJsonUnmarshaller();
		JsonResponseHandler<GetItemResult> responseHandler = new JsonResponseHandler<GetItemResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

    @Override
        public GetItemResult getItem(String tableName, java.util.Map<String,AttributeValue> key, Boolean consistentRead)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("getItem using String, Map, and Boolean is not implemented in Alternator.");
        }

    @Override
        public GetItemResult getItem(String tableName, java.util.Map<String,AttributeValue> key)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("getItem using String and Map is not implemented in Alternator.");
        }

    @Override
	public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<BatchGetItemRequest> request = new BatchGetItemRequestMarshaller().marshall(batchGetItemRequest);

		Unmarshaller<BatchGetItemResult, JsonUnmarshallerContext> unmarshaller = new BatchGetItemResultJsonUnmarshaller();
		JsonResponseHandler<BatchGetItemResult> responseHandler = new JsonResponseHandler<BatchGetItemResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

    @Override
        public BatchGetItemResult batchGetItem(java.util.Map<String, KeysAndAttributes> requestItems)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("batchGetItem using Map is not implemented in Alternator.");
        }

    @Override
        public BatchGetItemResult batchGetItem(java.util.Map<String, KeysAndAttributes> requestItems, String returnConsumedCapacity)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("batchGetItem using Map and String is not implemented in Alternator.");
        }

    @Override
	public ListTablesResult listTables() throws AmazonServiceException, AmazonClientException {
		return listTables(new ListTablesRequest());
	}

    @Override
	public void setEndpoint(String endpoint) throws IllegalArgumentException {
            URI endpointUri;
            try {
                endpointUri = new URI(endpoint);
            } catch (URISyntaxException ex) {
                throw new IllegalArgumentException(String.format("Invalid endpoint: %s", endpoint), ex);
            }
            synchronized (this) {
                this.endpoint = endpointUri;
            }
	}

        /**
         * Internal method for implementing {@link #getServiceName()}. Method is
         * protected by intent so peculiar subclass that don't follow the class
         * naming convention can choose to return whatever service name as needed.
         */
    @Override
        protected String getServiceNameIntern() {
            return "dynamo";
        }

    @Override
        public ListTablesResult listTables(Integer limit)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("listTables using Integer is not implemented in Alternator.");
        }

    @Override
        public ListTablesResult listTables(String exclusiveStartTableName, Integer limit)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("listTables using String and Integer is not implemented in Alternator.");
        }

    @Override
        public ListTablesResult listTables(String exclusiveStartTableName)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("listTables using String is not implemented in Alternator.");
        }

    @Override
	public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
		return client.getResponseMetadataForRequest(request);
	}

	private <X, Y extends AmazonWebServiceRequest> X invoke(Request<Y> request, HttpResponseHandler<AmazonWebServiceResponse<X>> responseHandler) {
		request.setEndpoint(endpoint);

		AmazonWebServiceRequest originalRequest = request.getOriginalRequest();

		ExecutionContext executionContext = createExecutionContext(request);
//		executionContext.setCustomBackoffStrategy(com.amazonaws.internal.DynamoDBBackoffStrategy.DEFAULT);
		JsonErrorResponseHandler errorResponseHandler = new JsonErrorResponseHandler(exceptionUnmarshallers);

		Response<X> result = client.execute(request, responseHandler, errorResponseHandler, executionContext);
                return result.getAwsResponse();
	}
}

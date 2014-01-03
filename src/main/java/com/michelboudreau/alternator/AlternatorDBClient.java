package com.michelboudreau.alternator;

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
import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodb.model.BatchGetItemResult;
import com.amazonaws.services.dynamodb.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodb.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.CreateTableResult;
import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemResult;
import com.amazonaws.services.dynamodb.model.DeleteTableRequest;
import com.amazonaws.services.dynamodb.model.DeleteTableResult;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableResult;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.services.dynamodb.model.GetItemResult;
import com.amazonaws.services.dynamodb.model.ListTablesRequest;
import com.amazonaws.services.dynamodb.model.ListTablesResult;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.amazonaws.services.dynamodb.model.UpdateItemResult;
import com.amazonaws.services.dynamodb.model.UpdateTableRequest;
import com.amazonaws.services.dynamodb.model.UpdateTableResult;
import com.amazonaws.services.dynamodb.model.transform.BatchGetItemRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.BatchGetItemResultJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.BatchWriteItemRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.BatchWriteItemResultJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.ConditionalCheckFailedExceptionUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.CreateTableRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.CreateTableResultJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.DeleteItemRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.DeleteItemResultJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.DeleteTableRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.DeleteTableResultJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.DescribeTableRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.DescribeTableResultJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.GetItemRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.GetItemResultJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.InternalServerErrorExceptionUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.LimitExceededExceptionUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.ListTablesRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.ListTablesResultJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.ProvisionedThroughputExceededExceptionUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.PutItemRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.PutItemResultJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.QueryRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.QueryResultJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.ResourceInUseExceptionUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.ResourceNotFoundExceptionUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.ScanRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.ScanResultJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.UpdateItemRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.UpdateItemResultJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.UpdateTableRequestMarshaller;
import com.amazonaws.services.dynamodb.model.transform.UpdateTableResultJsonUnmarshaller;
import com.amazonaws.transform.JsonErrorUnmarshaller;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.Unmarshaller;
import com.amazonaws.util.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public class AlternatorDBClient extends AmazonWebServiceClient implements AmazonDynamoDB {
	private static final Log log = LogFactory.getLog(AlternatorDBClient.class);
	protected List<Unmarshaller<AmazonServiceException, JSONObject>> exceptionUnmarshallers;

	public AlternatorDBClient() {
		this(new ClientConfiguration());
	}

	public AlternatorDBClient(ClientConfiguration clientConfiguration) {
		super(clientConfiguration);
		init();
	}

	private void init() {
		exceptionUnmarshallers = new ArrayList<Unmarshaller<AmazonServiceException, JSONObject>>();
		exceptionUnmarshallers.add(new LimitExceededExceptionUnmarshaller());
		exceptionUnmarshallers.add(new InternalServerErrorExceptionUnmarshaller());
		exceptionUnmarshallers.add(new ProvisionedThroughputExceededExceptionUnmarshaller());
		exceptionUnmarshallers.add(new ResourceInUseExceptionUnmarshaller());
		exceptionUnmarshallers.add(new ConditionalCheckFailedExceptionUnmarshaller());
		exceptionUnmarshallers.add(new ResourceNotFoundExceptionUnmarshaller());

		exceptionUnmarshallers.add(new JsonErrorUnmarshaller());
		setEndpoint("http://localhost:9090/");

		HandlerChainFactory chainFactory = new HandlerChainFactory();
		requestHandler2s.addAll(chainFactory.newRequestHandlerChain("/com/amazonaws/services/dynamodb/request.handlers"));

		clientConfiguration = new ClientConfiguration(clientConfiguration);
		if (clientConfiguration.getRetryPolicy() == ClientConfiguration.DEFAULT_RETRY_POLICY) {
			log.debug("Overriding default max error retry value to: " + 10);
			clientConfiguration.setMaxErrorRetry(10);
		}
		setConfiguration(clientConfiguration);
	}

	public ListTablesResult listTables(ListTablesRequest listTablesRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<ListTablesRequest> request = new ListTablesRequestMarshaller().marshall(listTablesRequest);

		Unmarshaller<ListTablesResult, JsonUnmarshallerContext> unmarshaller = new ListTablesResultJsonUnmarshaller();
		JsonResponseHandler<ListTablesResult> responseHandler = new JsonResponseHandler<ListTablesResult>(unmarshaller);

		ListTablesResult result = invoke(request, responseHandler);
                return result;
	}

	public QueryResult query(QueryRequest queryRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<QueryRequest> request = new QueryRequestMarshaller().marshall(queryRequest);

		Unmarshaller<QueryResult, JsonUnmarshallerContext> unmarshaller = new QueryResultJsonUnmarshaller();
		JsonResponseHandler<QueryResult> responseHandler = new JsonResponseHandler<QueryResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

	public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<BatchWriteItemRequest> request = new BatchWriteItemRequestMarshaller().marshall(batchWriteItemRequest);

		Unmarshaller<BatchWriteItemResult, JsonUnmarshallerContext> unmarshaller = new BatchWriteItemResultJsonUnmarshaller();
		JsonResponseHandler<BatchWriteItemResult> responseHandler = new JsonResponseHandler<BatchWriteItemResult>(unmarshaller);

		return invoke(request, responseHandler);
}

	public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<UpdateItemRequest> request = new UpdateItemRequestMarshaller().marshall(updateItemRequest);

		Unmarshaller<UpdateItemResult, JsonUnmarshallerContext> unmarshaller = new UpdateItemResultJsonUnmarshaller();
		JsonResponseHandler<UpdateItemResult> responseHandler = new JsonResponseHandler<UpdateItemResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

	public PutItemResult putItem(PutItemRequest putItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<PutItemRequest> request = new PutItemRequestMarshaller().marshall(putItemRequest);

		Unmarshaller<PutItemResult, JsonUnmarshallerContext> unmarshaller = new PutItemResultJsonUnmarshaller();
		JsonResponseHandler<PutItemResult> responseHandler = new JsonResponseHandler<PutItemResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

	public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<DescribeTableRequest> request = new DescribeTableRequestMarshaller().marshall(describeTableRequest);

		Unmarshaller<DescribeTableResult, JsonUnmarshallerContext> unmarshaller = new DescribeTableResultJsonUnmarshaller();
		JsonResponseHandler<DescribeTableResult> responseHandler = new JsonResponseHandler<DescribeTableResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

	public ScanResult scan(ScanRequest scanRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<ScanRequest> request = new ScanRequestMarshaller().marshall(scanRequest);

		Unmarshaller<ScanResult, JsonUnmarshallerContext> unmarshaller = new ScanResultJsonUnmarshaller();
		JsonResponseHandler<ScanResult> responseHandler = new JsonResponseHandler<ScanResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

	public CreateTableResult createTable(CreateTableRequest createTableRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<CreateTableRequest> request = new CreateTableRequestMarshaller().marshall(createTableRequest);

		Unmarshaller<CreateTableResult, JsonUnmarshallerContext> unmarshaller = new CreateTableResultJsonUnmarshaller();
		JsonResponseHandler<CreateTableResult> responseHandler = new JsonResponseHandler<CreateTableResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

	public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<UpdateTableRequest> request = new UpdateTableRequestMarshaller().marshall(updateTableRequest);

		Unmarshaller<UpdateTableResult, JsonUnmarshallerContext> unmarshaller = new UpdateTableResultJsonUnmarshaller();
		JsonResponseHandler<UpdateTableResult> responseHandler = new JsonResponseHandler<UpdateTableResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

	public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<DeleteTableRequest> request = new DeleteTableRequestMarshaller().marshall(deleteTableRequest);

		Unmarshaller<DeleteTableResult, JsonUnmarshallerContext> unmarshaller = new DeleteTableResultJsonUnmarshaller();
		JsonResponseHandler<DeleteTableResult> responseHandler = new JsonResponseHandler<DeleteTableResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

	public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<DeleteItemRequest> request = new DeleteItemRequestMarshaller().marshall(deleteItemRequest);

		Unmarshaller<DeleteItemResult, JsonUnmarshallerContext> unmarshaller = new DeleteItemResultJsonUnmarshaller();
		JsonResponseHandler<DeleteItemResult> responseHandler = new JsonResponseHandler<DeleteItemResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

	public GetItemResult getItem(GetItemRequest getItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<GetItemRequest> request = new GetItemRequestMarshaller().marshall(getItemRequest);

		Unmarshaller<GetItemResult, JsonUnmarshallerContext> unmarshaller = new GetItemResultJsonUnmarshaller();
		JsonResponseHandler<GetItemResult> responseHandler = new JsonResponseHandler<GetItemResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

	public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<BatchGetItemRequest> request = new BatchGetItemRequestMarshaller().marshall(batchGetItemRequest);

		Unmarshaller<BatchGetItemResult, JsonUnmarshallerContext> unmarshaller = new BatchGetItemResultJsonUnmarshaller();
		JsonResponseHandler<BatchGetItemResult> responseHandler = new JsonResponseHandler<BatchGetItemResult>(unmarshaller);

		return invoke(request, responseHandler);
	}

	public ListTablesResult listTables() throws AmazonServiceException, AmazonClientException {
		return listTables(new ListTablesRequest());
	}

	@Override
	public void setEndpoint(String endpoint) throws IllegalArgumentException {
		super.setEndpoint(endpoint, "dynamodb", null);
	}

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

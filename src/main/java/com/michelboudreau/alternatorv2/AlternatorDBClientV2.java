package com.michelboudreau.alternatorv2;

import com.amazonaws.*;
import com.amazonaws.handlers.HandlerChainFactory;
import com.amazonaws.http.ExecutionContext;
import com.amazonaws.http.HttpResponseHandler;
import com.amazonaws.http.JsonErrorResponseHandler;
import com.amazonaws.http.JsonResponseHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.model.transform.*;
import com.amazonaws.transform.JsonErrorUnmarshaller;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.Unmarshaller;
import com.amazonaws.util.json.JSONObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AlternatorDBClientV2 extends AmazonWebServiceClient implements AmazonDynamoDB {
	private static final Log log = LogFactory.getLog(AlternatorDBClientV2.class);
	protected List<Unmarshaller<AmazonServiceException, JSONObject>> exceptionUnmarshallers;

	public AlternatorDBClientV2() {
		this(new ClientConfiguration());
	}

	public AlternatorDBClientV2(ClientConfiguration clientConfiguration) {
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
		requestHandlers.addAll(chainFactory.newRequestHandlerChain("/com/amazonaws/services/dynamodb/request.handlers"));


		clientConfiguration = new ClientConfiguration(clientConfiguration);
		if (clientConfiguration.getMaxErrorRetry() == ClientConfiguration.DEFAULT_MAX_RETRIES) {
			log.debug("Overriding default max error retry value to: " + 10);
			clientConfiguration.setMaxErrorRetry(10);
		}
		setConfiguration(clientConfiguration);
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
	public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<UpdateItemRequest> request = new UpdateItemRequestMarshaller().marshall(updateItemRequest);

		Unmarshaller<UpdateItemResult, JsonUnmarshallerContext> unmarshaller = new UpdateItemResultJsonUnmarshaller();
		JsonResponseHandler<UpdateItemResult> responseHandler = new JsonResponseHandler<UpdateItemResult>(unmarshaller);

		return invoke(request, responseHandler);
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
	public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<DescribeTableRequest> request = new DescribeTableRequestMarshaller().marshall(describeTableRequest);

		Unmarshaller<DescribeTableResult, JsonUnmarshallerContext> unmarshaller = new DescribeTableResultJsonUnmarshaller();
		JsonResponseHandler<DescribeTableResult> responseHandler = new JsonResponseHandler<DescribeTableResult>(unmarshaller);

		return invoke(request, responseHandler);
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
	public CreateTableResult createTable(CreateTableRequest createTableRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<CreateTableRequest> request = new CreateTableRequestMarshaller().marshall(createTableRequest);

		Unmarshaller<CreateTableResult, JsonUnmarshallerContext> unmarshaller = new CreateTableResultJsonUnmarshaller();
		JsonResponseHandler<CreateTableResult> responseHandler = new JsonResponseHandler<CreateTableResult>(unmarshaller);

		return invoke(request, responseHandler);
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
	public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<DeleteTableRequest> request = new DeleteTableRequestMarshaller().marshall(deleteTableRequest);

		Unmarshaller<DeleteTableResult, JsonUnmarshallerContext> unmarshaller = new DeleteTableResultJsonUnmarshaller();
		JsonResponseHandler<DeleteTableResult> responseHandler = new JsonResponseHandler<DeleteTableResult>(unmarshaller);

		return invoke(request, responseHandler);
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
	public GetItemResult getItem(GetItemRequest getItemRequest)
			throws AmazonServiceException, AmazonClientException {
		Request<GetItemRequest> request = new GetItemRequestMarshaller().marshall(getItemRequest);

		Unmarshaller<GetItemResult, JsonUnmarshallerContext> unmarshaller = new GetItemResultJsonUnmarshaller();
		JsonResponseHandler<GetItemResult> responseHandler = new JsonResponseHandler<GetItemResult>(unmarshaller);

		return invoke(request, responseHandler);
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
	public ListTablesResult listTables() throws AmazonServiceException, AmazonClientException {
		return listTables(new ListTablesRequest());
	}

	@Override
	public void setEndpoint(String endpoint) throws IllegalArgumentException {
		super.setEndpoint(endpoint);
	}

    @Override
	public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
		return client.getResponseMetadataForRequest(request);
	}

	private <X, Y extends AmazonWebServiceRequest> X invoke(Request<Y> request, HttpResponseHandler<AmazonWebServiceResponse<X>> responseHandler) {
		request.setEndpoint(endpoint);

		AmazonWebServiceRequest originalRequest = request.getOriginalRequest();

		ExecutionContext executionContext = createExecutionContext();
		executionContext.setCustomBackoffStrategy(com.amazonaws.internal.DynamoDBBackoffStrategy.DEFAULT);
		JsonErrorResponseHandler errorResponseHandler = new JsonErrorResponseHandler(exceptionUnmarshallers);

		return (X) client.execute(request, responseHandler, errorResponseHandler, executionContext);
	}
}

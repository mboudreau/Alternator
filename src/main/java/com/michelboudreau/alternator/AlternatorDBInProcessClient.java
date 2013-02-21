package com.michelboudreau.alternator;

import com.amazonaws.*;
import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AlternatorDBInProcessClient extends AmazonWebServiceClient implements AmazonDynamoDB {
	private static final Log log = LogFactory.getLog(AlternatorDBInProcessClient.class);

	private AlternatorDBHandler handler = new AlternatorDBHandler();

	public AlternatorDBInProcessClient() {
		this(new ClientConfiguration());
	}

	public AlternatorDBInProcessClient(ClientConfiguration clientConfiguration) {
		super(clientConfiguration);
		init();
	}

	private void init() {
	}

	public ListTablesResult listTables(ListTablesRequest listTablesRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.listTables(listTablesRequest);
	}

	public QueryResult query(QueryRequest queryRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.query(queryRequest);
	}

	public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.batchWriteItem(batchWriteItemRequest);
	}

	public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.updateItem(updateItemRequest);
	}

	public PutItemResult putItem(PutItemRequest putItemRequest)
			throws AmazonServiceException, AmazonClientException, ConditionalCheckFailedException {
		return handler.putItem(putItemRequest);
	}

	public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.describeTable(describeTableRequest);
	}

	public ScanResult scan(ScanRequest scanRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.scan(scanRequest);
	}

	public CreateTableResult createTable(CreateTableRequest createTableRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.createTable(createTableRequest);
	}

	public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.updateTable(updateTableRequest);
	}

	public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.deleteTable(deleteTableRequest);
	}

	public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.deleteItem(deleteItemRequest);
	}

	public GetItemResult getItem(GetItemRequest getItemRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.getItem(getItemRequest);
	}

	public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.batchGetItem(batchGetItemRequest);
	}

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
}

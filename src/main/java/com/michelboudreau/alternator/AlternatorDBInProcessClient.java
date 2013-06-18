package com.michelboudreau.alternator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ResponseMetadata;
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
			throws AmazonServiceException, AmazonClientException {

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
		return handler.listTables(new ListTablesRequest());
	}

	@Override
	public void setEndpoint(String endpoint) throws IllegalArgumentException {
		super.setEndpoint(endpoint);
	}

	@Override
	public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
		return client.getResponseMetadataForRequest(request);
	}

	public void save(String persistence) {
            handler.save(persistence);
	}

	public void restore(String persistence) {
            handler.restore(persistence);
	}
}

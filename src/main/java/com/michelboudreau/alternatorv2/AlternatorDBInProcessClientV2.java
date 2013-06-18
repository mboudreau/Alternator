package com.michelboudreau.alternatorv2;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
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
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
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
import com.michelboudreau.alternator.AlternatorDBHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AlternatorDBInProcessClientV2 extends AmazonWebServiceClient implements AmazonDynamoDB {
	private static final Log log = LogFactory.getLog(AlternatorDBInProcessClientV2.class);

	private AlternatorDBHandler handler = new AlternatorDBHandler();

	public AlternatorDBInProcessClientV2() {
		this(new ClientConfiguration());
	}

	public AlternatorDBInProcessClientV2(ClientConfiguration clientConfiguration) {
		super(clientConfiguration);
		init();
	}

	private void init() {
	}

	public ListTablesResult listTables(ListTablesRequest listTablesRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.listTablesV2(listTablesRequest);
	}

	public QueryResult query(QueryRequest queryRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.queryV2(queryRequest);
	}

	public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.batchWriteItemV2(batchWriteItemRequest);
	}

	public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.updateItemV2(updateItemRequest);
	}

	public PutItemResult putItem(PutItemRequest putItemRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.putItemV2(putItemRequest);
}

	public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.describeTableV2(describeTableRequest);
	}

	public ScanResult scan(ScanRequest scanRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.scanV2(scanRequest);
	}

	public CreateTableResult createTable(CreateTableRequest createTableRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.createTableV2(createTableRequest);
	}

	public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.updateTableV2(updateTableRequest);
	}

	public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.deleteTableV2(deleteTableRequest);
	}

	public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.deleteItemV2(deleteItemRequest);
    }

	public GetItemResult getItem(GetItemRequest getItemRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.getItemV2(getItemRequest);
	}

	public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.batchGetItemV2(batchGetItemRequest);
	}

	public ListTablesResult listTables() throws AmazonServiceException, AmazonClientException {
		return handler.listTablesV2(new ListTablesRequest());
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

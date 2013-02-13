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

        try {
            return handler.listTables(listTablesRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new ListTablesResult();
        }
	}

	public QueryResult query(QueryRequest queryRequest)
			throws AmazonServiceException, AmazonClientException {

        try {
            return handler.query(queryRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new QueryResult();
        }
	}

	public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest)
			throws AmazonServiceException, AmazonClientException {

        try {
            return handler.batchWriteItem(batchWriteItemRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new BatchWriteItemResult();
        }
}

	public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest)
			throws AmazonServiceException, AmazonClientException {

        try {
            return handler.updateItem(updateItemRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new UpdateItemResult();
        }
	}

	public PutItemResult putItem(PutItemRequest putItemRequest)
			throws AmazonServiceException, AmazonClientException {

        try {
            return handler.putItem(putItemRequest);
	    } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new PutItemResult();
        }
}

	public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.describeTable(describeTableRequest);
	}

	public ScanResult scan(ScanRequest scanRequest)
			throws AmazonServiceException, AmazonClientException {

        try {
            return handler.scan(scanRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new ScanResult();
        }
	}

	public CreateTableResult createTable(CreateTableRequest createTableRequest)
			throws AmazonServiceException, AmazonClientException {

        try {
            return handler.createTable(createTableRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new CreateTableResult();
        }
	}

	public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest)
			throws AmazonServiceException, AmazonClientException {

        try {
            return handler.updateTable(updateTableRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new UpdateTableResult();
        }
	}

	public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest)
			throws AmazonServiceException, AmazonClientException {

        try {
            return handler.deleteTable(deleteTableRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new DeleteTableResult();
        }
	}

	public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest)
			throws AmazonServiceException, AmazonClientException {

        try {
            return handler.deleteItem(deleteItemRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new DeleteItemResult();
        }
    }

	public GetItemResult getItem(GetItemRequest getItemRequest)
			throws AmazonServiceException, AmazonClientException {

        try {
            return handler.getItem(getItemRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new GetItemResult();
        }
	}

	public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest)
			throws AmazonServiceException, AmazonClientException {

        try {
            return handler.batchGetItem(batchGetItemRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new BatchGetItemResult();
        }
	}

	public ListTablesResult listTables() throws AmazonServiceException, AmazonClientException {
        try {
            return listTables(new ListTablesRequest());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new ListTablesResult();
        }
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

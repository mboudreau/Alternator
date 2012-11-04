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

        try {
            return handler.describeTable(describeTableRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new DescribeTableResult();
        }
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
        
	public void save(String persistence) {
            handler.save(persistence);
	}

	public void restore(String persistence) {
            handler.restore(persistence);
	}
}

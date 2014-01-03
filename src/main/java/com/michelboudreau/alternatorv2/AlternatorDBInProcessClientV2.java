package com.michelboudreau.alternatorv2;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ResponseMetadata;
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

        public BatchWriteItemResult batchWriteItem(java.util.Map<String,java.util.List<WriteRequest>> requestItems)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("batchWriteItem using Map is not implemented in Alternator.");
        }

	public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.updateItemV2(updateItemRequest);
	}

        public UpdateItemResult updateItem(String tableName, java.util.Map<String, AttributeValue> key, java.util.Map<String, AttributeValueUpdate> attributeUpdates, String returnValues)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("updateItem using String, Map, Map, and String is not implemented in Alternator.");
        }

        public UpdateItemResult updateItem(String tableName, java.util.Map<String,AttributeValue> key, java.util.Map<String,AttributeValueUpdate> attributeUpdates)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("updateItem using String, Map, and Map is not implemented in Alternator.");
        }

	public PutItemResult putItem(PutItemRequest putItemRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.putItemV2(putItemRequest);
}

        public PutItemResult putItem(String tableName, java.util.Map<String,AttributeValue> item, String returnValues)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("putItem using String, Map, and String is not implemented in Alternator.");
        }

        public PutItemResult putItem(String tableName, java.util.Map<String,AttributeValue> item)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("putItem using String and Map is not implemented in Alternator.");
        }

	public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.describeTableV2(describeTableRequest);
	}

       public DescribeTableResult describeTable(String tableName)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("describeTable using String is not implemented in Alternator.");
        }

	public ScanResult scan(ScanRequest scanRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.scanV2(scanRequest);
	}

        public ScanResult scan(String tableName, java.util.List<String> attributesToGet, java.util.Map<String,Condition> scanFilter)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("scan using String, List, and Map is not implemented in Alternator.");
        }

        public ScanResult scan(String tableName, java.util.Map<String,Condition> scanFilter)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("scan using String and Map is not implemented in Alternator.");
        }

        public ScanResult scan(String tableName, java.util.List<String> attributesToGet)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("scan using String and List is not implemented in Alternator.");
        }

	public CreateTableResult createTable(CreateTableRequest createTableRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.createTableV2(createTableRequest);
	}

        public CreateTableResult createTable(java.util.List<AttributeDefinition> attributeDefinitions, String tableName, java.util.List<KeySchemaElement> keySchema, ProvisionedThroughput provisionedThroughput)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("createTable using List, String, List, and ProvisionedThroughput is not implemented in Alternator.");
        }

	public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.updateTableV2(updateTableRequest);
	}

        public UpdateTableResult updateTable(String tableName, ProvisionedThroughput provisionedThroughput)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("updateTable using String and ProvisionedThroughput is not implemented in Alternator.");
        }

	public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.deleteTableV2(deleteTableRequest);
	}

        public DeleteTableResult deleteTable(String tableName)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("deleteTable using String is not implemented in Alternator.");
        }

	public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.deleteItemV2(deleteItemRequest);
        }

        public DeleteItemResult deleteItem(String tableName, java.util.Map<String,AttributeValue> key, String returnValues)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("deleteTable using String, Map, and String is not implemented in Alternator.");
        }

        public DeleteItemResult deleteItem(String tableName, java.util.Map<String,AttributeValue> key)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("deleteItem using String and Map is not implemented in Alternator.");
        }

	public GetItemResult getItem(GetItemRequest getItemRequest)
			throws AmazonServiceException, AmazonClientException {

            return handler.getItemV2(getItemRequest);
	}

        public GetItemResult getItem(String tableName, java.util.Map<String,AttributeValue> key, Boolean consistentRead)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("getItem using String, Map, and Boolean is not implemented in Alternator.");
        }

        public GetItemResult getItem(String tableName, java.util.Map<String,AttributeValue> key)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("getItem using String and Map is not implemented in Alternator.");
        }

	public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest)
			throws AmazonServiceException, AmazonClientException {
		return handler.batchGetItemV2(batchGetItemRequest);
	}

        public BatchGetItemResult batchGetItem(java.util.Map<String, KeysAndAttributes> requestItems)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("batchGetItem using Map is not implemented in Alternator.");
        }

        public BatchGetItemResult batchGetItem(java.util.Map<String, KeysAndAttributes> requestItems, String returnConsumedCapacity)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("batchGetItem using Map and String is not implemented in Alternator.");
        }

	public ListTablesResult listTables() throws AmazonServiceException, AmazonClientException {
		return handler.listTablesV2(new ListTablesRequest());
	}

        public ListTablesResult listTables(Integer limit)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("listTables using Integer is not implemented in Alternator.");
        }

        public ListTablesResult listTables(String exclusiveStartTableName, Integer limit)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("listTables using String and Integer is not implemented in Alternator.");
        }

        public ListTablesResult listTables(String exclusiveStartTableName)
                 throws AmazonServiceException, AmazonClientException {

                 throw new AmazonClientException("listTables using String is not implemented in Alternator.");
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

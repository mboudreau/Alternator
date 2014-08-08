package com.michelboudreau.alternator;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodb.model.AttributeAction;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodb.model.BatchGetItemResult;
import com.amazonaws.services.dynamodb.model.BatchResponse;
import com.amazonaws.services.dynamodb.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodb.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodb.model.BatchWriteResponse;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.CreateTableResult;
import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemResult;
import com.amazonaws.services.dynamodb.model.DeleteRequest;
import com.amazonaws.services.dynamodb.model.DeleteTableRequest;
import com.amazonaws.services.dynamodb.model.DeleteTableResult;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableResult;
import com.amazonaws.services.dynamodb.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.services.dynamodb.model.GetItemResult;
import com.amazonaws.services.dynamodb.model.InternalServerErrorException;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.KeysAndAttributes;
import com.amazonaws.services.dynamodb.model.LimitExceededException;
import com.amazonaws.services.dynamodb.model.ListTablesRequest;
import com.amazonaws.services.dynamodb.model.ListTablesResult;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.PutRequest;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ResourceInUseException;
import com.amazonaws.services.dynamodb.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodb.model.ReturnValue;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.TableStatus;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.amazonaws.services.dynamodb.model.UpdateItemResult;
import com.amazonaws.services.dynamodb.model.UpdateTableRequest;
import com.amazonaws.services.dynamodb.model.UpdateTableResult;
import com.amazonaws.services.dynamodb.model.WriteRequest;
import com.amazonaws.services.dynamodb.model.transform.BatchGetItemRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.BatchGetItemResultMarshaller;
import com.amazonaws.services.dynamodb.model.transform.BatchWriteItemRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.BatchWriteItemResultMarshaller;
import com.amazonaws.services.dynamodb.model.transform.CreateTableRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.CreateTableResultMarshaller;
import com.amazonaws.services.dynamodb.model.transform.DeleteItemRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.DeleteItemResultMarshaller;
import com.amazonaws.services.dynamodb.model.transform.DeleteTableRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.DeleteTableResultMarshaller;
import com.amazonaws.services.dynamodb.model.transform.DescribeTableRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.DescribeTableResultMarshaller;
import com.amazonaws.services.dynamodb.model.transform.GetItemRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.GetItemResultMarshaller;
import com.amazonaws.services.dynamodb.model.transform.ListTablesRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.ListTablesResultMarshaller;
import com.amazonaws.services.dynamodb.model.transform.PutItemRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.PutItemResultMarshaller;
import com.amazonaws.services.dynamodb.model.transform.QueryRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.QueryResultMarshaller;
import com.amazonaws.services.dynamodb.model.transform.ScanRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.ScanResultMarshaller;
import com.amazonaws.services.dynamodb.model.transform.UpdateItemRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.UpdateItemResultMarshaller;
import com.amazonaws.services.dynamodb.model.transform.UpdateTableRequestJsonUnmarshaller;
import com.amazonaws.services.dynamodb.model.transform.UpdateTableResultMarshaller;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.michelboudreau.alternator.enums.AttributeValueType;
import com.michelboudreau.alternator.enums.RequestType;
import com.michelboudreau.alternator.models.ItemRangeGroup;
import com.michelboudreau.alternator.models.Limits;
import com.michelboudreau.alternator.models.Table;
import com.michelboudreau.alternator.parsers.AmazonWebServiceRequestParser;
import com.michelboudreau.alternator.validators.CreateTableRequestValidator;
import com.michelboudreau.alternator.validators.DeleteItemRequestValidator;
import com.michelboudreau.alternator.validators.DeleteTableRequestValidator;
import com.michelboudreau.alternator.validators.DescribeTableRequestValidator;
import com.michelboudreau.alternator.validators.GetItemRequestValidator;
import com.michelboudreau.alternator.validators.ListTablesRequestValidator;
import com.michelboudreau.alternator.validators.PutItemRequestValidator;
import com.michelboudreau.alternator.validators.QueryRequestValidator;
import com.michelboudreau.alternator.validators.ScanRequestValidator;
import com.michelboudreau.alternator.validators.UpdateItemRequestValidator;
import com.michelboudreau.alternator.validators.UpdateTableRequestValidator;
import com.michelboudreau.alternatorv2.AlternatorDBApiVersion2Mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

public class AlternatorDBHandler {
    public final String API_VERSION_20111205 = "DynamoDB_20111205";
    public final String API_VERSION_20120810 = "DynamoDB_20120810";

	private final Logger logger = LoggerFactory.getLogger(AlternatorDBHandler.class);

	private Map<String, Table> tables = new HashMap<String, Table>();
	private List<Table> tableList = new ArrayList<Table>();

	// TODO: create constructor that can handle a file
	public AlternatorDBHandler() {
	}

	// Maybe save automatically on destroy?
	public synchronized void save(String persistence) {
		try {
			createObjectMapper().writeValue(new File(persistence), tableList);
		} catch (IOException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
	}

	public synchronized void restore(String persistence) {
		try {
			File dbFile = new File(persistence);
			if (dbFile.exists() == false) {
				return;
			}

			ObjectMapper objectMapper = createObjectMapper();
			tableList = objectMapper.readValue(dbFile, objectMapper.getTypeFactory().constructCollectionType(ArrayList.class, Table.class));

			for (Table table : tableList) {
				tables.put(table.getName(), table);
			}
		} catch (IOException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
	}

	// Not sure about this.  If correct and only need one, only create one instance
	public ObjectMapper createObjectMapper() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
				.setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.ANY)
				.setVisibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.NONE)
				.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE)
				.setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE);

		mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

		return mapper;
	}

	public String handle(HttpServletRequest request) throws LimitExceededException, InternalServerErrorException, ResourceInUseException, ResourceNotFoundException, ConditionalCheckFailedException {
        try {
            AmazonWebServiceRequestParser parser = new AmazonWebServiceRequestParser(request);

            String apiVersion = parser.getApiVersion();
            if (API_VERSION_20111205.equals(apiVersion)) {
                return handle20111205(request, parser);
            } else if (API_VERSION_20120810.equals(apiVersion)) {
                return handle20120810(request, parser);
            } else {
                String logMessage = "The API Version " + apiVersion + " is not supported.";
                logger.warn(logMessage);
                throw new AmazonServiceException(logMessage);
            }
        } catch (NullPointerException ex) {
            StackTraceElement[] stackTrace = ex.getStackTrace();
            StackTraceElement exceptionSource = stackTrace[0];
            String errorMessage = "Caught " + ex.getClass().getName() +
                    " (" + ex.getMessage() +
                    ") at " + exceptionSource.getClassName() +
                    "." + exceptionSource.getMethodName() +
                    " line " + exceptionSource.getLineNumber();
            logger.error(errorMessage);
            throw new AmazonServiceException(errorMessage);
        }
	}

	private String handle20111205(HttpServletRequest request, AmazonWebServiceRequestParser parser) throws LimitExceededException, InternalServerErrorException, ResourceInUseException, ResourceNotFoundException, ConditionalCheckFailedException {
        RequestType requestType = parser.getType();
		switch (requestType) {
			// Tables
			case CREATE_TABLE:
				return new CreateTableResultMarshaller().marshall(createTable(parser.getData(CreateTableRequest.class, CreateTableRequestJsonUnmarshaller.getInstance())));
			case DESCRIBE_TABLE:
				return new DescribeTableResultMarshaller().marshall(describeTable(parser.getData(DescribeTableRequest.class, DescribeTableRequestJsonUnmarshaller.getInstance())));
			case LIST_TABLES:
				return new ListTablesResultMarshaller().marshall(listTables(parser.getData(ListTablesRequest.class, ListTablesRequestJsonUnmarshaller.getInstance())));
			case UPDATE_TABLE:
				return new UpdateTableResultMarshaller().marshall(updateTable(parser.getData(UpdateTableRequest.class, UpdateTableRequestJsonUnmarshaller.getInstance())));
			case DELETE_TABLE:
				return new DeleteTableResultMarshaller().marshall(deleteTable(parser.getData(DeleteTableRequest.class, DeleteTableRequestJsonUnmarshaller.getInstance())));

			// Items
			case PUT:
				return new PutItemResultMarshaller().marshall(putItem(parser.getData(PutItemRequest.class, PutItemRequestJsonUnmarshaller.getInstance())));
			case GET:
				return new GetItemResultMarshaller().marshall(getItem(parser.getData(GetItemRequest.class, GetItemRequestJsonUnmarshaller.getInstance())));

			case UPDATE:
				return new UpdateItemResultMarshaller().marshall(updateItem(parser.getData(UpdateItemRequest.class, UpdateItemRequestJsonUnmarshaller.getInstance())));
			case DELETE:
				return new DeleteItemResultMarshaller().marshall(deleteItem(parser.getData(DeleteItemRequest.class, DeleteItemRequestJsonUnmarshaller.getInstance())));
			case BATCH_GET_ITEM:
				return new BatchGetItemResultMarshaller().marshall((batchGetItem(parser.getData(BatchGetItemRequest.class, BatchGetItemRequestJsonUnmarshaller.getInstance()))));
			case BATCH_WRITE_ITEM:
				return new BatchWriteItemResultMarshaller().marshall((batchWriteItem(parser.getData(BatchWriteItemRequest.class, BatchWriteItemRequestJsonUnmarshaller.getInstance()))));

			// Operations
			case QUERY:
				return new QueryResultMarshaller().marshall(query(parser.getData(QueryRequest.class, QueryRequestJsonUnmarshaller.getInstance())));
			case SCAN:
				return new ScanResultMarshaller().marshall(scan(parser.getData(ScanRequest.class, ScanRequestJsonUnmarshaller.getInstance())));
			default:
                String logMessage = "The Request Type '" + parser.getType() + "' does not exist.";
                logger.warn(logMessage);
                throw new AmazonServiceException(logMessage);
		}
	}

	private String handle20120810(HttpServletRequest request, AmazonWebServiceRequestParser parser) throws LimitExceededException, InternalServerErrorException, ResourceInUseException, ResourceNotFoundException, ConditionalCheckFailedException {
        RequestType requestType = parser.getType();
		switch (requestType) {
			// Tables
			case CREATE_TABLE:
				return new com.amazonaws.services.dynamodbv2.model.transform.CreateTableResultMarshaller().marshall(createTableV2(parser.getData(com.amazonaws.services.dynamodbv2.model.CreateTableRequest.class, com.amazonaws.services.dynamodbv2.model.transform.CreateTableRequestJsonUnmarshaller.getInstance())));
			case DESCRIBE_TABLE:
				return new com.amazonaws.services.dynamodbv2.model.transform.DescribeTableResultMarshaller().marshall(describeTableV2(parser.getData(com.amazonaws.services.dynamodbv2.model.DescribeTableRequest.class, com.amazonaws.services.dynamodbv2.model.transform.DescribeTableRequestJsonUnmarshaller.getInstance())));
			case LIST_TABLES:
				return new com.amazonaws.services.dynamodbv2.model.transform.ListTablesResultMarshaller().marshall(listTablesV2(parser.getData(com.amazonaws.services.dynamodbv2.model.ListTablesRequest.class, com.amazonaws.services.dynamodbv2.model.transform.ListTablesRequestJsonUnmarshaller.getInstance())));
			case UPDATE_TABLE:
				return new com.amazonaws.services.dynamodbv2.model.transform.UpdateTableResultMarshaller().marshall(updateTableV2(parser.getData(com.amazonaws.services.dynamodbv2.model.UpdateTableRequest.class, com.amazonaws.services.dynamodbv2.model.transform.UpdateTableRequestJsonUnmarshaller.getInstance())));
			case DELETE_TABLE:
				return new com.amazonaws.services.dynamodbv2.model.transform.DeleteTableResultMarshaller().marshall(deleteTableV2(parser.getData(com.amazonaws.services.dynamodbv2.model.DeleteTableRequest.class, com.amazonaws.services.dynamodbv2.model.transform.DeleteTableRequestJsonUnmarshaller.getInstance())));

			// Items
			case PUT:
				return new com.amazonaws.services.dynamodbv2.model.transform.PutItemResultMarshaller().marshall(putItemV2(parser.getData(com.amazonaws.services.dynamodbv2.model.PutItemRequest.class, com.amazonaws.services.dynamodbv2.model.transform.PutItemRequestJsonUnmarshaller.getInstance())));
			case GET:
				return new com.amazonaws.services.dynamodbv2.model.transform.GetItemResultMarshaller().marshall(getItemV2(parser.getData(com.amazonaws.services.dynamodbv2.model.GetItemRequest.class, com.amazonaws.services.dynamodbv2.model.transform.GetItemRequestJsonUnmarshaller.getInstance())));

			case UPDATE:
				return new com.amazonaws.services.dynamodbv2.model.transform.UpdateItemResultMarshaller().marshall(updateItemV2(parser.getData(com.amazonaws.services.dynamodbv2.model.UpdateItemRequest.class, com.amazonaws.services.dynamodbv2.model.transform.UpdateItemRequestJsonUnmarshaller.getInstance())));
			case DELETE:
				return new com.amazonaws.services.dynamodbv2.model.transform.DeleteItemResultMarshaller().marshall(deleteItemV2(parser.getData(com.amazonaws.services.dynamodbv2.model.DeleteItemRequest.class, com.amazonaws.services.dynamodbv2.model.transform.DeleteItemRequestJsonUnmarshaller.getInstance())));
			case BATCH_GET_ITEM:
				return new com.amazonaws.services.dynamodbv2.model.transform.BatchGetItemResultMarshaller().marshall(batchGetItemV2(parser.getData(com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest.class, com.amazonaws.services.dynamodbv2.model.transform.BatchGetItemRequestJsonUnmarshaller.getInstance())));
			case BATCH_WRITE_ITEM:
				return new com.amazonaws.services.dynamodbv2.model.transform.BatchWriteItemResultMarshaller().marshall(batchWriteItemV2(parser.getData(com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest.class, com.amazonaws.services.dynamodbv2.model.transform.BatchWriteItemRequestJsonUnmarshaller.getInstance())));

			// Operations
			case QUERY:
				return new com.amazonaws.services.dynamodbv2.model.transform.QueryResultMarshaller().marshall(queryV2(parser.getData(com.amazonaws.services.dynamodbv2.model.QueryRequest.class, com.amazonaws.services.dynamodbv2.model.transform.QueryRequestJsonUnmarshaller.getInstance())));
			case SCAN:
				return new com.amazonaws.services.dynamodbv2.model.transform.ScanResultMarshaller().marshall(scanV2(parser.getData(com.amazonaws.services.dynamodbv2.model.ScanRequest.class, com.amazonaws.services.dynamodbv2.model.transform.ScanRequestJsonUnmarshaller.getInstance())));
			default:
                String logMessage = "The Request Type '" + parser.getType() + "' does not exist.";
                logger.warn(logMessage);
                throw new AmazonServiceException(logMessage);
		}
	}

	public synchronized CreateTableResult createTable(CreateTableRequest request) throws LimitExceededException, InternalServerErrorException, ResourceInUseException {
		// table limit of 256
		if (this.tables.size() >= Limits.TABLE_MAX) {
			throw new LimitExceededException("Cannot exceed 256 tables per account.");
		}

		// Validate data coming in
		// TODO: Look into how we're doing validation, maybe implement better solution
		CreateTableRequestValidator validator = new CreateTableRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw new AmazonServiceException(errors.toString());
		}

		// get information
		String tableName = request.getTableName();

		// Check to make sure table with same name doesn't exist
		if (this.tables.containsKey(tableName)) {
			throw new ResourceInUseException("The table you're currently trying to create (" + tableName + ") is already available.");
		}

		// Add table to map, array
		Table table = new Table(tableName, request.getKeySchema(), request.getProvisionedThroughput());
		this.tables.put(tableName, table);
		this.tableList.add(table);

		return new CreateTableResult().withTableDescription(table.getTableDescription());
	}

	public com.amazonaws.services.dynamodbv2.model.CreateTableResult createTableV2(com.amazonaws.services.dynamodbv2.model.CreateTableRequest v2Request) throws LimitExceededException, InternalServerErrorException, ResourceInUseException {
        CreateTableRequest request = AlternatorDBApiVersion2Mapper.MapV2CreateTableRequestToV1(v2Request);
        try {
            CreateTableResult result = createTable(request);
            return AlternatorDBApiVersion2Mapper.MapV1CreateTableResultToV2(result);
        } catch (ResourceInUseException ex) {
            throw new com.amazonaws.services.dynamodbv2.model.ResourceInUseException(ex.getMessage());
        }
	}

	public synchronized DescribeTableResult describeTable(DescribeTableRequest request) throws InternalServerErrorException, ResourceNotFoundException {
		// Validate data coming in
		DescribeTableRequestValidator validator = new DescribeTableRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw new AmazonServiceException(errors.toString());
		}

		// get information
		String tableName = request.getTableName();
		DescribeTableResult result = null;

		// Check to make sure table with same name doesn't exist
		if (this.tables.containsKey(tableName)) {
			Table table = this.tables.get(tableName);
			result = new DescribeTableResult().withTable(table.getTableDescription());
		} else {
			throw new ResourceNotFoundException("The table '" + tableName + "' does not exist.");
		}
		return result;
	}

	public com.amazonaws.services.dynamodbv2.model.DescribeTableResult describeTableV2(com.amazonaws.services.dynamodbv2.model.DescribeTableRequest v2Request) throws InternalServerErrorException, ResourceNotFoundException {
        DescribeTableRequest request = AlternatorDBApiVersion2Mapper.MapV2DescribeTableRequestToV1(v2Request);
        DescribeTableResult result = describeTable(request);
        return AlternatorDBApiVersion2Mapper.MapV1DescribeTableResultToV2(result);
	}

	public synchronized ListTablesResult listTables(ListTablesRequest request) throws InternalServerErrorException, ResourceNotFoundException {
		// Validate data coming in
		ListTablesRequestValidator validator = new ListTablesRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw createInternalServerException(errors);
		}

		// Create defaults
		String startTableName = request.getExclusiveStartTableName();
		Integer limit = request.getLimit();
		if (limit == null) {
			limit = 100;
		}

		// Check if startTableName exists
		int startIndex = 0;
		if (startTableName != null) {
			if (this.tables.containsKey(startTableName)) {
				for (int i = 0; i < this.tableList.size(); i++) {
					if (tableList.get(i).getName().equals(startTableName)) {
						startIndex = i;
						break;
					}
				}
			} else {
				throw new ResourceNotFoundException("The ExclusiveStartTableName '" + startTableName + "' doesn't exist.");
			}
		}

		// Calculate size max. depending on array size and limit
		int size = this.tableList.size();
		Boolean setTableName = false;
		if (size > (startIndex + limit)) {
			size = startIndex + limit;
			setTableName = true;
		}

		// Get list
		List<String> tables = new ArrayList<String>();
		for (int i = startIndex; i < size; i++) {
			tables.add(this.tableList.get(i).getName());
		}

		// Create result object
		ListTablesResult result = new ListTablesResult().withTableNames(tables);
		if (setTableName) {
			result.setLastEvaluatedTableName(this.tableList.get(size).getName());
		}

		return result;
	}

	public com.amazonaws.services.dynamodbv2.model.ListTablesResult listTablesV2(com.amazonaws.services.dynamodbv2.model.ListTablesRequest v2Request) throws InternalServerErrorException, ResourceNotFoundException {
        ListTablesRequest request = AlternatorDBApiVersion2Mapper.MapV2ListTablesRequestToV1(v2Request);
        ListTablesResult result = listTables(request);
        return AlternatorDBApiVersion2Mapper.MapV1ListTablesResultToV2(result);
	}

	public synchronized DeleteTableResult deleteTable(DeleteTableRequest request) throws InternalServerErrorException, ResourceNotFoundException {
		// Validate data coming in
		DeleteTableRequestValidator validator = new DeleteTableRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw new AmazonServiceException(errors.toString());
		}

		// Check existence
		if (!this.tables.containsKey(request.getTableName())) {
			throw new ResourceNotFoundException("The table you want to delete '" + request.getTableName() + "' doesn't exist.");
		}

		// Delete Table
		Table table = tables.remove(request.getTableName());
		tableList.remove(table);

		return new DeleteTableResult().withTableDescription(table.getTableDescription().withTableStatus(TableStatus.DELETING));
	}

	public com.amazonaws.services.dynamodbv2.model.DeleteTableResult deleteTableV2(com.amazonaws.services.dynamodbv2.model.DeleteTableRequest v2Request) throws InternalServerErrorException, ResourceNotFoundException {
        DeleteTableRequest request = AlternatorDBApiVersion2Mapper.MapV2DeleteTableRequestToV1(v2Request);
        try {
            DeleteTableResult result = deleteTable(request);
            return AlternatorDBApiVersion2Mapper.MapV1DeleteTableResultToV2(result);
        } catch (ResourceNotFoundException ex) {
            throw new com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException(ex.getMessage());
        }
	}

	public synchronized UpdateTableResult updateTable(UpdateTableRequest request) throws InternalServerErrorException, ResourceNotFoundException {
		// Validate data coming in
		UpdateTableRequestValidator validator = new UpdateTableRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw new AmazonServiceException(errors.toString());
		}

		// Check existence
		if (!this.tables.containsKey(request.getTableName())) {
			throw new ResourceNotFoundException("The table '" + request.getTableName() + "' doesn't exist.");
		}

		// Update Table
		Table table = this.tables.get(request.getTableName());
		table.setProvisionedThroughput(request.getProvisionedThroughput());

		return new UpdateTableResult().withTableDescription(table.getTableDescription());
	}

	public com.amazonaws.services.dynamodbv2.model.UpdateTableResult updateTableV2(com.amazonaws.services.dynamodbv2.model.UpdateTableRequest v2Request) throws InternalServerErrorException, ResourceNotFoundException {
        UpdateTableRequest request = AlternatorDBApiVersion2Mapper.MapV2UpdateTableRequestToV1(v2Request);
        UpdateTableResult result = updateTable(request);
        return AlternatorDBApiVersion2Mapper.MapV1UpdateTableResultToV2(result);
	}

	public synchronized PutItemResult putItem(PutItemRequest request) throws InternalServerErrorException, ResourceNotFoundException, ConditionalCheckFailedException {
		// Validate data coming in
		PutItemRequestValidator validator = new PutItemRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw new AmazonServiceException(errors.toString());

		}

		// Check existence of table
		Table table = this.tables.get(request.getTableName());
		if (table == null) {
			throw new ResourceNotFoundException("The table '" + request.getTableName() + "' doesn't exist.");
		}

		// Make sure that item specifies hash key and range key (if in schema)
		KeySchemaElement hashKey = table.getKeySchema().getHashKeyElement();
		KeySchemaElement rangeKey = table.getKeySchema().getRangeKeyElement();
		AttributeValue hashItem = request.getItem().get(hashKey.getAttributeName());
		AttributeValueType hashItemType = getAttributeValueType(hashItem);
		if (hashItem == null || hashItemType != AttributeValueType.fromString(hashKey.getAttributeType())) {
			throw new InternalServerErrorException("Missing hash key (" + hashKey.getAttributeName() + ") from item: " + request.getItem());
		}
		if (rangeKey != null) {
			AttributeValue rangeItem = request.getItem().get(rangeKey.getAttributeName());
			AttributeValueType rangeItemType = getAttributeValueType(rangeItem);
			if (rangeItem == null || rangeItemType != AttributeValueType.fromString(rangeKey.getAttributeType())) {
				throw new InternalServerErrorException("Missing range key (" + rangeKey.getAttributeName() + ") from item: " + request.getItem());
			}
		}

		// Get current item if it exists
//		Map<String, AttributeValue> item = table.getItem(getKeyValue(request.getItem().get(table.getHashKeyName())));
        Map<String, AttributeValue> requestItem = request.getItem();
        String hashKeyValue = getKeyValue(requestItem.get(table.getHashKeyName()));
        String rangeKeyValue = getKeyValue(requestItem.get(table.getRangeKeyName()));
		Map<String, AttributeValue> item = table.getItem(hashKeyValue, rangeKeyValue);

		// Check conditional put
		validateExpected(request.getExpected(), item);

		PutItemResult result = new PutItemResult().withConsumedCapacityUnits(1D);
		if (item != null && request.getReturnValues() != null && ReturnValue.fromValue(request.getReturnValues()) == ReturnValue.ALL_OLD) {
			result.setAttributes(item);
		}

		// put the item in the table
		table.putItem(request.getItem());

		return result;
	}

    private boolean compareAttributeValues(AttributeValue a, AttributeValue b) {
        if (a.getB() != null && a.getB().equals(b.getB())) {
            return true;
        } else if (a.getN() != null && a.getN().equals(b.getN())) {
            return true;
        } else if (a.getS() != null && a.getS().equals(b.getS())) {
            return true;
        } else if (a.getSS() != null && a.getSS().equals(b.getSS())) {
            return true;
        }

        return false;
    }

	public synchronized com.amazonaws.services.dynamodbv2.model.PutItemResult putItemV2(com.amazonaws.services.dynamodbv2.model.PutItemRequest v2Request) throws InternalServerErrorException, ResourceNotFoundException, ConditionalCheckFailedException {
        PutItemRequest request = AlternatorDBApiVersion2Mapper.MapV2PutItemRequestToV1(v2Request);
        try {
            PutItemResult result = putItem(request);
            return AlternatorDBApiVersion2Mapper.MapV1PutItemResultToV2(result, v2Request.getTableName());
        } catch (ConditionalCheckFailedException ex) {
            throw new com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException(ex.getMessage());
        }
	}

	public synchronized GetItemResult getItem(GetItemRequest request) throws InternalServerErrorException, ResourceNotFoundException {
		// Validate data coming in
		GetItemRequestValidator validator = new GetItemRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw new AmazonServiceException(errors.toString());
		}

		// get information
		String tableName = request.getTableName();
		Key key = request.getKey();
		List<String> attributesToGet = request.getAttributesToGet();
		GetItemResult result = new GetItemResult();

		// Check to make sure table exists
		if (!this.tables.containsKey(tableName)) {
			throw new ResourceNotFoundException("The table you're currently trying to access (" + tableName + ") doesn't exists.");
		}
		// Check to make sure Key is valid
		String hashKeyValue = getKeyValue(key.getHashKeyElement());
        ItemRangeGroup rangeGroup = this.tables.get(tableName).getItemRangeGroup(hashKeyValue);

		if (rangeGroup == null) {
			return new GetItemResult();
			// throw new ResourceNotFoundException("No item with Hash Key (" + hashKeyValue + ") exists.");
		} else {
            String rangeKeyValue = getKeyValue(key.getRangeKeyElement());
            Map<String, AttributeValue> item = this.tables.get(tableName).getItem(hashKeyValue, rangeKeyValue);
            if (item == null) {
				return new GetItemResult();
				// throw new ResourceNotFoundException("No item with Hash Key (" + hashKeyValue + ") and Range Key )" + rangeKeyValue + ") exists.");
            }

			if (attributesToGet == null) {
				result.setItem(item);
			} else {
                Map<String, AttributeValue> response = new HashMap<String, AttributeValue>();
				for (String att : attributesToGet) {
					AttributeValue res = item.get(att);
					if (res != null) {
						response.put(att, res);
					}
				}
				result.setItem(response);
			}
		}
		return result;
	}

	public com.amazonaws.services.dynamodbv2.model.GetItemResult getItemV2(com.amazonaws.services.dynamodbv2.model.GetItemRequest v2Request) throws InternalServerErrorException, ResourceNotFoundException {
        Table table = this.tables.get(v2Request.getTableName());
        GetItemRequest request = AlternatorDBApiVersion2Mapper.MapV2GetItemRequestToV1(v2Request, table);
        GetItemResult result = getItem(request);
        return AlternatorDBApiVersion2Mapper.MapV1GetItemResultToV2(result, v2Request.getTableName());
	}

	public synchronized DeleteItemResult deleteItem(DeleteItemRequest request) {
		// Validate data coming in
		DeleteItemRequestValidator validator = new DeleteItemRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw new AmazonServiceException(errors.toString());
		}

		// Check existence of table
		Table table = this.tables.get(request.getTableName());
		if (table == null) {
			throw new ResourceNotFoundException("The table '" + request.getTableName() + "' doesn't exist.");
		}

		// Get hash and range key
		String hashKey = getKeyValue(request.getKey().getHashKeyElement());
		String rangeKey = getKeyValue(request.getKey().getRangeKeyElement());

		// Get current item if exist
		Map<String, AttributeValue> item = table.getItem(hashKey, rangeKey);

		if (item == null) {
            if (rangeKey == null) {
                throw new ResourceNotFoundException("The item with hash key '" + hashKey + "' doesn't exist in table '" + table.getName() + "'");
            } else {
                throw new ResourceNotFoundException("The item with hash key '" + hashKey + "' and range key '" + rangeKey + "' doesn't exist in table '" + table.getName() + "'");
            }
		}

		// Check conditional put
		validateExpected(request.getExpected(), item);

		DeleteItemResult result = new DeleteItemResult().withConsumedCapacityUnits(1D);
		if (item != null && request.getReturnValues() != null && ReturnValue.fromValue(request.getReturnValues()) == ReturnValue.ALL_OLD) {
			result.setAttributes(item);
		}

		// remove the item from the table
		table.removeItem(hashKey, rangeKey);
		return result;
	}

	public com.amazonaws.services.dynamodbv2.model.DeleteItemResult deleteItemV2(com.amazonaws.services.dynamodbv2.model.DeleteItemRequest v2Request) {
        Table table = this.tables.get(v2Request.getTableName());
        DeleteItemRequest request = AlternatorDBApiVersion2Mapper.MapV2DeleteItemRequestToV1(v2Request, table);
        DeleteItemResult result = deleteItem(request);
        return AlternatorDBApiVersion2Mapper.MapV1DeleteItemResultToV2(result, v2Request.getTableName());
	}

	public synchronized BatchGetItemResult batchGetItem(BatchGetItemRequest request) {
		BatchGetItemResult batchGetItemResult = new BatchGetItemResult();
		Map<String, BatchResponse> response = new HashMap<String, BatchResponse>();
		for (String tableName : request.getRequestItems().keySet()) {
			BatchResponse batchResponse = new BatchResponse();
			List<Map<String, AttributeValue>> items = new ArrayList<Map<String, AttributeValue>>();
			KeysAndAttributes keysAndAttributes = request.getRequestItems().get(tableName);
			List<Key> itemKeys = keysAndAttributes.getKeys();
			List<String> attributeToGet = keysAndAttributes.getAttributesToGet();
			try {
				for (Key itemKey : itemKeys) {
					try {
                        String hashKeyValue = getKeyValue(itemKey.getHashKeyElement());
                        String rangeKeyValue = getKeyValue(itemKey.getRangeKeyElement());
						Map<String, AttributeValue> item = this.tables.get(tableName).getItem(hashKeyValue, rangeKeyValue);
						item = getItemWithAttributesToGet(item, attributeToGet);
						if (item != null)
							items.add(item);
					} catch (NullPointerException e) {
						System.err.println("Caught NullPointerException: " + e.getMessage());
					}
				}
			} catch (NullPointerException e) {
				System.err.println("Caught NullPointerException: " + e.getMessage());
			}
			batchResponse.setConsumedCapacityUnits(1.0);
			if (items.size() != 0) {
				batchResponse.setItems(items);
				response.put(tableName, batchResponse);
				batchGetItemResult.setResponses(response);
				batchGetItemResult.getResponses().put(tableName, batchResponse);
			}
		}
        batchGetItemResult.setUnprocessedKeys(new HashMap<String, KeysAndAttributes>());
		return batchGetItemResult;
	}

	public com.amazonaws.services.dynamodbv2.model.BatchGetItemResult batchGetItemV2(com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest v2Request) {
        BatchGetItemRequest request = AlternatorDBApiVersion2Mapper.MapV2BatchGetItemRequestToV1(v2Request, this.tables);
        BatchGetItemResult result = batchGetItem(request);
        final Set<String> requestedTables = v2Request.getRequestItems().keySet();
        return AlternatorDBApiVersion2Mapper.MapV1BatchGetItemResultToV2(result, this.tables, requestedTables);
	}

	public synchronized BatchWriteItemResult batchWriteItem(BatchWriteItemRequest request) {
		BatchWriteItemResult batchWriteItemResult = new BatchWriteItemResult();
		HashMap<String, BatchWriteResponse> responses = new HashMap<String, BatchWriteResponse>();
		for (String tableName : request.getRequestItems().keySet()) {
			BatchWriteResponse batchWriteResponse = new BatchWriteResponse();
			List<WriteRequest> writeRequests = request.getRequestItems().get(tableName);
			for (WriteRequest writeRequest : writeRequests) {
				PutRequest putRequest = writeRequest.getPutRequest();
				if (putRequest != null) {
					this.tables.get(tableName).putItem(putRequest.getItem());
				}
				DeleteRequest deleteRequest = writeRequest.getDeleteRequest();
				if (deleteRequest != null) {
					Key key = deleteRequest.getKey();
					if (key != null) {
						this.tables.get(tableName).removeItem(key.getHashKeyElement().getS());
					}
				}
			}
			batchWriteResponse.setConsumedCapacityUnits(1.0);
			responses.put(tableName, batchWriteResponse);
		}
        batchWriteItemResult.setResponses(responses);
        batchWriteItemResult.setUnprocessedItems(new HashMap<String, List<WriteRequest>>());
		return batchWriteItemResult;
	}

	public com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult batchWriteItemV2(com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest v2Request) {
     try {
		com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult batchWriteItemResult =
                new com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult();
        List<com.amazonaws.services.dynamodbv2.model.ConsumedCapacity> v2Capacities =
                new ArrayList<com.amazonaws.services.dynamodbv2.model.ConsumedCapacity>();
		for (String tableName : v2Request.getRequestItems().keySet()) {
			List<com.amazonaws.services.dynamodbv2.model.WriteRequest> writeRequests = v2Request.getRequestItems().get(tableName);
			for (com.amazonaws.services.dynamodbv2.model.WriteRequest writeRequest : writeRequests) {
				com.amazonaws.services.dynamodbv2.model.PutRequest putRequest = writeRequest.getPutRequest();
				if (putRequest != null) {
                    com.amazonaws.services.dynamodbv2.model.PutItemRequest putItemRequest =
                        new com.amazonaws.services.dynamodbv2.model.PutItemRequest()
                            .withTableName(tableName)
                            .withItem(putRequest.getItem())
                            ;
					putItemV2(putItemRequest);
				}
				com.amazonaws.services.dynamodbv2.model.DeleteRequest deleteRequest = writeRequest.getDeleteRequest();
				if (deleteRequest != null) {
                    com.amazonaws.services.dynamodbv2.model.DeleteItemRequest deleteItemRequest =
                        new com.amazonaws.services.dynamodbv2.model.DeleteItemRequest()
                            .withTableName(tableName)
                            .withKey(deleteRequest.getKey())
                            ;
                    deleteItemV2(deleteItemRequest);
				}
			}
            v2Capacities.add(
                new com.amazonaws.services.dynamodbv2.model.ConsumedCapacity()
                    .withTableName(tableName)
                    .withCapacityUnits(1.0)
                    );
		}
        batchWriteItemResult.setUnprocessedItems(
            new HashMap<String, List<com.amazonaws.services.dynamodbv2.model.WriteRequest>>()
            );
        batchWriteItemResult.setConsumedCapacity(v2Capacities);
		return batchWriteItemResult;
      } catch (ResourceNotFoundException rnfe) {
			throw new com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException(rnfe.getMessage());
      }
	}

    private boolean isComparableInScan(AttributeValue value, AttributeValue comp) {
        if (comp == null) return false;
        final AttributeValueType compType = getAttributeValueType(comp);
        final AttributeValueType valueType = getAttributeValueType(value);
        return (compType.equals(AttributeValueType.N) || compType.equals(AttributeValueType.S)) && compType.equals(valueType);
    }

    private int compareForScan(AttributeValue value, AttributeValue comp) {
        //Get type
        final AttributeValueType valueType = getAttributeValueType(value);
        if (valueType.equals(AttributeValueType.S)) return value.getS().compareTo(comp.getS());

        if (valueType.equals(AttributeValueType.N)) {
            try {
                return new Long(value.getN()).compareTo(new Long(comp.getN()));
            } catch (NumberFormatException e) {
                return new Double(value.getN()).compareTo(new Double(comp.getN()));
            }
        }

        throw new IllegalArgumentException("Can only compare String and Number types, got " + valueType);
    }

    private String getAttributeValueAsString(AttributeValue value) {
        final AttributeValueType valueType = getAttributeValueType(value);
        if (valueType.equals(AttributeValueType.N)) return value.getN();
        if (valueType.equals(AttributeValueType.S)) return value.getS();
        throw new IllegalArgumentException("Can only return values for Number and String, got " + valueType);
    }

	public synchronized ScanResult scan(ScanRequest request) {
		ScanResult result = new ScanResult();
		List<Error> errors = new ScanRequestValidator().validate(request);
		if (errors.size() > 0) {
			throw createInternalServerException(errors);
		}
		result.setConsumedCapacityUnits(0.5);
		List<Map<String, AttributeValue>> items = new ArrayList<Map<String, AttributeValue>>();
		for (String key : this.tables.get(request.getTableName()).getItemRangeGroups().keySet()) {
          ItemRangeGroup rangeGroup = this.tables.get(request.getTableName()).getItemRangeGroup(key);
          for (String rangeKey : rangeGroup.getKeySet()) {
			Map<String, AttributeValue> item = rangeGroup.getItem(rangeKey);
			if (request.getScanFilter() != null) {
                //Don't add item immediately - evaluate all conditions first
                boolean shouldAddItem = true;
				for (String k : request.getScanFilter().keySet()) {
                    //Set this to true if one of conditions matches
                    boolean conditionMatches = false;
                    final AttributeValue attribute = item.get(k);
					if (attribute != null) {
						final Condition cond = request.getScanFilter().get(k);
                        final AttributeValue comp = cond.getAttributeValueList().isEmpty() ? null : cond.getAttributeValueList().get(0);
                        final int condSize = cond.getAttributeValueList().size();

                        if (cond.getComparisonOperator() == null) {
							throw new ResourceNotFoundException("There must be a comparisonOperator");
						}

                        if (cond.getComparisonOperator().equals("EQ")) {
							if (condSize == 1 && isComparableInScan(attribute, comp)) {
								conditionMatches = compareForScan(attribute, comp) == 0;
							}
						} else
						if (cond.getComparisonOperator().equals("LE")) {
							if (condSize == 1 && isComparableInScan(attribute, comp)) {
                                conditionMatches = compareForScan(attribute, comp) <= 0;
						    }
						} else
						if (cond.getComparisonOperator().equals("LT")) {
							if (condSize == 1 && isComparableInScan(attribute, comp)) {
                                conditionMatches = compareForScan(attribute, comp) < 0;
							}
						} else
						if (cond.getComparisonOperator().equals("GE")) {
							if (condSize == 1 && isComparableInScan(attribute, comp)) {
                                conditionMatches = compareForScan(attribute, comp) >= 0;
                            }
						} else
						if (cond.getComparisonOperator().equals("GT")) {
							if (condSize == 1 && isComparableInScan(attribute, comp)) {
                                conditionMatches = compareForScan(attribute, comp) > 0;
							}
						} else
                        if (cond.getComparisonOperator().equals("BETWEEN")) {
                            if (condSize == 2) {
                                final AttributeValue comp2 = cond.getAttributeValueList().get(1);
                                if (isComparableInScan(attribute, comp) && isComparableInScan(attribute, comp2)) {
                                    conditionMatches = compareForScan(attribute, comp) >= 0 && compareForScan(attribute, comp2) <= 0;
                                }
                            }
                        } else
                        if (cond.getComparisonOperator().equals("BEGINS_WITH")) {
                            //TODO: Should we fail on AttributeValueType other than S (as per AWS DynamoDB docs)?
                            //TODO: Investigate how actual DynamoDB works
                            if (condSize == 1 && getAttributeValueType(attribute).equals(AttributeValueType.S) &&
                                    getAttributeValueType(comp).equals(AttributeValueType.S)) {
                                conditionMatches = attribute.getS().startsWith(comp.getS());
                            }
                        } else
                        if (cond.getComparisonOperator().equals("CONTAINS")) {
                            if (condSize == 1) {
								if (getAttributeValueType(item.get(k)).equals(AttributeValueType.S) || getAttributeValueType(item.get(k)).equals(AttributeValueType.N)) {
									String value = getAttributeValueAsString(attribute);
									String subs = getAttributeValueAsString(comp);
                                    conditionMatches = value.contains(subs);
                                }
                            }
                            //TODO: Check for sets!!!
                        } else
						if (cond.getComparisonOperator().equals("IN")) {
							for(AttributeValue value : cond.getAttributeValueList()){
								if (item.get(k).equals(value)){
                                    conditionMatches = true;
                                    break;
                                }
							}
						}
					}
                    shouldAddItem = shouldAddItem && conditionMatches;
				}
                if (shouldAddItem) items.add(item);
			} else {
				items.add(item);
			}
          }
		}
		if ((request.getLimit() != null) && (items.size() > request.getLimit())) {
			items = items.subList(0, request.getLimit() - 1);
		}

		if (request.getAttributesToGet() != null) {
			List<Map<String, AttributeValue>> copy = getItemWithAttributesToGet(items, request.getAttributesToGet());
			items = copy;
		}

		result.setItems(items);
		result.setCount(items.size());
		result.setScannedCount(items.size());
		return result;
	}

	public com.amazonaws.services.dynamodbv2.model.ScanResult scanV2(com.amazonaws.services.dynamodbv2.model.ScanRequest v2Request) {
        Table table = this.tables.get(v2Request.getTableName());
        ScanRequest request = AlternatorDBApiVersion2Mapper.MapV2ScanRequestToV1(v2Request, table);
        ScanResult result = scan(request);
        return AlternatorDBApiVersion2Mapper.MapV1ScanResultToV2(result, table);
	}

	public synchronized QueryResult query(QueryRequest request) {
		// Validate data coming in
		QueryRequestValidator validator = new QueryRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw createInternalServerException(errors);
		}

		// Check existence of table
		Table table = this.tables.get(request.getTableName());
		if (table == null) {
			throw new ResourceNotFoundException("The table '" + request.getTableName() + "' doesn't exist.");
		}

        String hashKeyValue = getKeyValue(request.getHashKeyValue());
        List<String> attributesToGet = request.getAttributesToGet();

		QueryResult queryResult = new QueryResult();
		List<Map<String, AttributeValue>> list = new ArrayList<Map<String, AttributeValue>>();

        KeySchema keySchema = table.getKeySchema();
        KeySchemaElement rangeKeyElement = keySchema.getRangeKeyElement();
        ItemRangeGroup rangeGroup = table.getItemRangeGroup(hashKeyValue);
        if (rangeGroup != null) {
			for (Map<String, AttributeValue> item : rangeGroup.getItems(rangeKeyElement, request.getRangeKeyCondition())) {
				if (request.getScanIndexForward() == null || request.getScanIndexForward() == true) {
                    // The default value is true (forward).
                    // If ScanIndexForward is not specified, the results are returned in ascending order.
					list.add(getItemWithAttributesToGet(item, attributesToGet));
				} else {
					list.add(0, getItemWithAttributesToGet(item, attributesToGet));
				}
            }
        }

		if (request.getLimit() != null && request.getLimit() > 0) {
			while (list.size() > request.getLimit()) {
				list.remove((int) request.getLimit());
			}
		}


		queryResult.setItems(list);
		queryResult.setCount(list.size());
		queryResult.setConsumedCapacityUnits(0.5);

        // DynamoDBMapper implements paged query continuations if we return a LastEvaluatedKey value.
        // Leave this value null to indicate we are returning the full result set.
		queryResult.setLastEvaluatedKey(null);  // new Key(request.getHashKeyValue()));

		return queryResult;
	}

	public com.amazonaws.services.dynamodbv2.model.QueryResult queryV2(com.amazonaws.services.dynamodbv2.model.QueryRequest v2Request) {
        Table table = this.tables.get(v2Request.getTableName());
        QueryRequest request = AlternatorDBApiVersion2Mapper.MapV2QueryRequestToV1(v2Request, table);
        QueryResult result = query(request);
        return AlternatorDBApiVersion2Mapper.MapV1QueryResultToV2(result, table);
	}

	public String getKeyValue(AttributeValue value) {
		if (value != null) {
			if (value.getN() != null) {
				return value.getN();
			} else if (value.getS() != null) {
				return value.getS();
			}
		}
		return null;
	}

	public AttributeValueType getAttributeValueType(AttributeValue value) {
		if (value != null) {
			if (value.getN() != null) {
				return AttributeValueType.N;
			} else if (value.getS() != null) {
				return AttributeValueType.S;
			} else if (value.getNS() != null) {
				return AttributeValueType.NS;
			} else if (value.getSS() != null) {
				return AttributeValueType.SS;
			}
		}
		return AttributeValueType.UNKNOWN;
	}

	public InternalServerErrorException createInternalServerException(List<Error> errors) {
		String message = "The following Errors occured: ";
		for (Error error : errors) {
			message += error.getMessage() + "\n";
		}
		return new InternalServerErrorException(message);
	}

	public synchronized UpdateItemResult updateItem(UpdateItemRequest request) {
		// Validate data coming in
		// TODO: Look into how we're doing validation, maybe implement better solution
		UpdateItemRequestValidator validator = new UpdateItemRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw new AmazonServiceException(errors.toString());
		}

		// get information
		String tableName = request.getTableName();
		Key key = request.getKey();
		Map<String, ExpectedAttributeValue> expected = request.getExpected();
		Map<String, AttributeValueUpdate> attributesToUpdate = request.getAttributeUpdates();
		String returnValues = request.getReturnValues();


		UpdateItemResult result = new UpdateItemResult();
		result.setConsumedCapacityUnits(0.5);

		// Check to make sure table exists
		if (!this.tables.containsKey(tableName)) {
			throw new ResourceNotFoundException("The table you're currently trying to access (" + tableName + ") doesn't exists.");
		}
		// Check to make sure Key is valid
        String hashKeyValue = getKeyValue(key.getHashKeyElement());
        String rangeKeyValue = getKeyValue(key.getRangeKeyElement());
        Map<String, AttributeValue> item = this.tables.get(tableName).getItem(hashKeyValue, rangeKeyValue);
       
		// Check conditional put
        validateExpected(request.getExpected(), item);
        
		if (item == null) {
			item = new HashMap<String, AttributeValue>();
			item.put(this.tables.get(tableName).getHashKeyName(), key.getHashKeyElement());
			if (key.getRangeKeyElement() != null) {
				item.put(this.tables.get(tableName).getRangeKeyName(), key.getRangeKeyElement());
			}
			for (String sKey : attributesToUpdate.keySet()) {
				if (attributesToUpdate.get(sKey).getValue() != null) {
					item.put(sKey, attributesToUpdate.get(sKey).getValue());
				}
			}
			this.tables.get(tableName).putItem(item);
			result.setAttributes(item);
		} else {
			Set<String> sKeyz = new HashSet<String>(item.keySet());
			sKeyz.addAll(attributesToUpdate.keySet());
			for (String sKey : sKeyz) {
				if (attributesToUpdate.containsKey(sKey)) {
					if (attributesToUpdate.get(sKey).getAction().equalsIgnoreCase(AttributeAction.PUT.name())) {
						item.remove(sKey);
						item.put(sKey, attributesToUpdate.get(sKey).getValue());
						attributesToUpdate.remove(sKey);
					} else if (attributesToUpdate.get(sKey).getAction().equalsIgnoreCase(AttributeAction.DELETE.name())) {
						if (attributesToUpdate.get(sKey).getValue() != null) {
							deleteAttributeValue(item, sKey, attributesToUpdate.get(sKey));
						} else {
							item.remove(sKey);
						}
						attributesToUpdate.remove(sKey);
					} else if (attributesToUpdate.get(sKey).getAction().equalsIgnoreCase(AttributeAction.ADD.name())) {
						if (attributesToUpdate.get(sKey).getValue() != null) {
							addAttributeValue(item, sKey, attributesToUpdate.get(sKey));
						} else {
							throw new ResourceNotFoundException("the provided update item with attribute (" + sKey + ") doesn't have an AttributeValue to perform the ADD");
						}
					}
				}
			}
			result.setAttributes(item);
		}
		return result;
	}

	public com.amazonaws.services.dynamodbv2.model.UpdateItemResult updateItemV2(com.amazonaws.services.dynamodbv2.model.UpdateItemRequest v2Request) {
        Table table = this.tables.get(v2Request.getTableName());
        UpdateItemRequest request = AlternatorDBApiVersion2Mapper.MapV2UpdateItemRequestToV1(v2Request, table);
		try {
			UpdateItemResult result = updateItem(request);
			return AlternatorDBApiVersion2Mapper.MapV1UpdateItemResultToV2(result, v2Request.getTableName());
		} catch (ConditionalCheckFailedException ccfev1) {
			throw new com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException(ccfev1.getMessage());
		}
	}

	public Map<String, AttributeValue> getItemWithAttributesToGet(Map<String, AttributeValue> item, List<String> attributesToGet) {
		if (item == null) {
			return item;
		}
		if (attributesToGet == null) {
			return item;
		}
		Set<String> attributes = new HashSet<String>(item.keySet());
		for (String attribute : attributes) {
			if (!attributesToGet.contains(attribute)) {
				item.remove(attribute);
			}
		}
		return item;
	}

	public List<Map<String, AttributeValue>> getItemWithAttributesToGet(List<Map<String, AttributeValue>> items, List<String> attributesToGet) {
		List<Map<String, AttributeValue>> copy = new ArrayList<Map<String, AttributeValue>>();
		for (Map<String, AttributeValue> item : items) {
			copy.add(getItemWithAttributesToGet(item, attributesToGet));
		}
		return copy;
	}

	private void addAttributeValue(Map<String, AttributeValue> item, String attributename, AttributeValueUpdate valueUpdate){

		AttributeValue value = item.get(attributename);
		if (value==null){
			//new field
			value = valueUpdate.getValue();
			item.put(attributename, value);
		}else{
			if (value.getSS() != null) {
				if (valueUpdate.getValue().getSS() == null) {
					throw new ConditionalCheckFailedException("It's not possible to ADD something else than a List<String> for the attribute (" + attributename + ")");
				} else {
					for (String toUp : valueUpdate.getValue().getSS()) {
						value.getSS().add(toUp);
					}
				}
			} else if (value.getNS() != null) {
				if (valueUpdate.getValue().getNS() == null) {
					throw new ConditionalCheckFailedException("It's not possible to ADD something else than a List<Number> for the attribute (" + attributename + ")");
				} else {
					for (String toUp : valueUpdate.getValue().getNS()) {
						value.getNS().add(toUp);
					}
				}
			} else if (value.getS() != null) {
				throw new ConditionalCheckFailedException("It's not possible to ADD on an attribute with a String type for the attribute (" + attributename + ")");
			} else if (value.getN() != null) {
                try {
                    Long l = Long.valueOf(value.getN());
                    l = l + Long.valueOf(valueUpdate.getValue().getN());
                    value.setN(l + "");
                } catch (NumberFormatException e) {
                    Double i = new Double(value.getN());
                    i = i + new Double(valueUpdate.getValue().getN());
                    value.setN(i + "");
                }
			}
		}
	}

	private void deleteAttributeValue(Map<String, AttributeValue> item, String attributename, AttributeValueUpdate valueToDelete){
		AttributeValue existingValue = item.get(attributename);
		if (existingValue == null){
			return; //do nothing. need to confirm with live dynamodb behaviour.
		}
		if (existingValue.getSS() != null) {
			if (valueToDelete.getValue().getSS() == null) {
				throw new ConditionalCheckFailedException("It's not possible to delete something else than a List<String> for the attribute (" + attributename + ") of the item with hash key (" + existingValue + ")");
			} else {
				for (String toDel : valueToDelete.getValue().getSS()) {
					if (existingValue.getSS().contains(toDel)) {
						existingValue.getSS().remove(toDel);
					}
				}
			}
		} else if (existingValue.getNS() != null) {
			if (valueToDelete.getValue().getNS() == null) {
				throw new ConditionalCheckFailedException("It's not possible to delete something else than a List<Number> for the attribute (" + attributename + ") of the item with hash key (" + existingValue + ")");
			} else {
				for (String toDel : valueToDelete.getValue().getNS()) {
					if (existingValue.getNS().contains(toDel)) {
						existingValue.getNS().remove(toDel);
					}
				}
			}
		} else if (existingValue.getS()!=null && existingValue.getS().equals(valueToDelete.getValue().getS())) {
			item.remove(attributename);
		} else if (existingValue.getN()!=null && existingValue.getN().equals(valueToDelete.getValue().getN())) {
			item.remove(attributename);
		}
	}
	
	private void validateExpected(Map<String, ExpectedAttributeValue> expected, Map<String, AttributeValue> item) {
		if (expected != null) {
			for (Map.Entry<String, ExpectedAttributeValue> entry : expected.entrySet()) {
				String key = entry.getKey();
				ExpectedAttributeValue value = entry.getValue();
				value.setExists(value.getValue() != null);
				if ((value.getExists() && item == null) || (!value.getExists() && item != null)) {
					throw new ConditionalCheckFailedException("The exist conditional could not be met.");
				}
				if (value.getValue() != null) {
					// check to see if value conditional is equal
					if (!compareAttributeValues(value.getValue(), item.get(key))) {
						throw new ConditionalCheckFailedException("The value conditional could is not equal");
					}
				}
			}
		}
	}
}

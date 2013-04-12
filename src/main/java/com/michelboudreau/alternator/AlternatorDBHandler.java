package com.michelboudreau.alternator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.michelboudreau.alternator.enums.AttributeValueType;
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

class AlternatorDBHandler {

	private final Logger logger = LoggerFactory.getLogger(AlternatorDBHandler.class);

	private Map<String, Table> tables = new HashMap<String, Table>();
	private List<Table> tableList = new ArrayList<Table>();

	// TODO: create constructor that can handle a file
	public AlternatorDBHandler() {
	}

	// Maybe save automatically on destroy?
	public void save(String persistence) {
		try {
			createObjectMapper().writeValue(new File(persistence), tableList);
		} catch (IOException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
	}

	public void restore(String persistence) {
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
		mapper.setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY)
				.setVisibility(JsonMethod.CREATOR, JsonAutoDetect.Visibility.ANY)
				.setVisibility(JsonMethod.SETTER, JsonAutoDetect.Visibility.NONE)
				.setVisibility(JsonMethod.GETTER, JsonAutoDetect.Visibility.NONE)
				.setVisibility(JsonMethod.IS_GETTER, JsonAutoDetect.Visibility.NONE);

		mapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);

		return mapper;
	}

	public String handle(HttpServletRequest request) throws LimitExceededException, InternalServerErrorException, ResourceInUseException, ResourceNotFoundException, ConditionalCheckFailedException {
		AmazonWebServiceRequestParser parser = new AmazonWebServiceRequestParser(request);

		switch (parser.getType()) {
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
				logger.warn("The Request Type '" + parser.getType() + "' does not exist.");
				break;
		}
		return null;
	}

	public CreateTableResult createTable(CreateTableRequest request) throws LimitExceededException, InternalServerErrorException, ResourceInUseException {
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

	public DescribeTableResult describeTable(DescribeTableRequest request) throws InternalServerErrorException, ResourceNotFoundException {
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

	public ListTablesResult listTables(ListTablesRequest request) throws InternalServerErrorException, ResourceNotFoundException {
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

	public DeleteTableResult deleteTable(DeleteTableRequest request) throws InternalServerErrorException, ResourceNotFoundException {
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

	public UpdateTableResult updateTable(UpdateTableRequest request) throws InternalServerErrorException, ResourceNotFoundException {
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

	public PutItemResult putItem(PutItemRequest request) throws InternalServerErrorException, ResourceNotFoundException, ConditionalCheckFailedException {
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
		if (request.getExpected() != null) {
			for (Map.Entry<String, ExpectedAttributeValue> entry : request.getExpected().entrySet()) {
				String key = entry.getKey();
				ExpectedAttributeValue value = entry.getValue();
				value.setExists(value.getValue() != null);
				if ((value.getExists() && item == null) || (!value.getExists() && item != null)) {
					throw new ConditionalCheckFailedException("The exist conditional could not be met.");
				}
				if (value.getValue() != null) {
					// check to see if value conditional is equal
					if (
							(value.getValue().getN() != null && !item.get(key).equals(value.getValue().getN())) || (value.getValue().getS() != null && !item.get(key).equals(value.getValue().getS())) || (value.getValue().getNS() != null && !item.get(key).equals(value.getValue().getNS())) || (value.getValue().getSS() != null && !item.get(key).equals(value.getValue().getSS()))
							) {
						throw new ConditionalCheckFailedException("The value conditional could is not equal");
					}
				}
			}
		}

		PutItemResult result = new PutItemResult().withConsumedCapacityUnits(1D);
		if (item != null && request.getReturnValues() != null && ReturnValue.fromValue(request.getReturnValues()) == ReturnValue.ALL_OLD) {
			result.setAttributes(item);
		}

		// put the item in the table
		table.putItem(request.getItem());

		return result;
	}

	public GetItemResult getItem(GetItemRequest request) throws InternalServerErrorException, ResourceNotFoundException {
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

	public DeleteItemResult deleteItem(DeleteItemRequest request) {
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
		if (request.getExpected() != null) {
			for (Map.Entry<String, ExpectedAttributeValue> entry : request.getExpected().entrySet()) {
				String key = entry.getKey();
				ExpectedAttributeValue value = entry.getValue();
				value.setExists(value.getValue() != null);
				if ((value.getExists() && item == null) || (!value.getExists() && item != null)) {
					throw new ConditionalCheckFailedException("The exist conditional could not be met.");
				}
				if (value.getValue() != null) {
					// check to see if value conditional is equal
					if (
							(value.getValue().getN() != null && !item.get(key).equals(value.getValue().getN())) || (value.getValue().getS() != null && !item.get(key).equals(value.getValue().getS())) || (value.getValue().getNS() != null && !item.get(key).equals(value.getValue().getNS())) || (value.getValue().getSS() != null && !item.get(key).equals(value.getValue().getSS()))
							) {
						throw new ConditionalCheckFailedException("The value conditional could is not equal");
					}
				}
			}
		}

		DeleteItemResult result = new DeleteItemResult().withConsumedCapacityUnits(1D);
		if (item != null && request.getReturnValues() != null && ReturnValue.fromValue(request.getReturnValues()) == ReturnValue.ALL_OLD) {
			result.setAttributes(item);
		}

		// remove the item from the table
		table.removeItem(hashKey, rangeKey);
		return result;
	}

	public BatchGetItemResult batchGetItem(BatchGetItemRequest request) {
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
		return batchGetItemResult;
	}

	public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest request) {
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
			batchWriteItemResult.setResponses(responses);
			batchWriteItemResult.getResponses().put(tableName, batchWriteResponse);
            batchWriteItemResult.setUnprocessedItems(new HashMap<String, List<WriteRequest>>());
		}
		return batchWriteItemResult;
	}

	public ScanResult scan(ScanRequest request) {
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
				for (String k : request.getScanFilter().keySet()) {
					if (item.get(k) != null) {
						Condition cond = request.getScanFilter().get(k);
						if (cond.getComparisonOperator() == null) {
							throw new ResourceNotFoundException("There must be a comparisonOperator");
						}
						if (cond.getComparisonOperator().equals("EQ")) {
							if (cond.getAttributeValueList().size() == 1) {
								if (item.get(k).equals(cond.getAttributeValueList().get(0))) {
									items.add(item);
								}
							} else {
								if (item.get(k).equals(cond.getAttributeValueList())) {
									items.add(item);
								}
							}
						}
						if (cond.getComparisonOperator().equals("LE")) {
							if (cond.getAttributeValueList().size() == 1) {
								if (getAttributeValueType(item.get(k)).equals(AttributeValueType.S) || getAttributeValueType(item.get(k)).equals(AttributeValueType.N)) {
									String value = (getAttributeValueType(item.get(k)).equals(AttributeValueType.S)) ? item.get(k).getS() : item.get(k).getN();
									String comp = (getAttributeValueType(cond.getAttributeValueList().get(0)).equals(AttributeValueType.S)) ? cond.getAttributeValueList().get(0).getS() : cond.getAttributeValueList().get(0).getN();
									if (value.compareTo(comp) >= 0) {
										items.add(item);
									}
								} else {
									//TODO to do
									//List<String> value = (getAttributeValueType(item.get(k)).equals(AttributeValueType.SS))? item.get(k).getSS() : item.get(k).getNS();
								}

							} else {
								//TODO to do
								if (item.get(k).equals(cond.getAttributeValueList())) {
									items.add(item);
								}
							}
						}
						if (cond.getComparisonOperator().equals("LT")) {
							if (cond.getAttributeValueList().size() == 1) {
								if (getAttributeValueType(item.get(k)).equals(AttributeValueType.S) || getAttributeValueType(item.get(k)).equals(AttributeValueType.N)) {
									String value = (getAttributeValueType(item.get(k)).equals(AttributeValueType.S)) ? item.get(k).getS() : item.get(k).getN();
									String comp = (getAttributeValueType(cond.getAttributeValueList().get(0)).equals(AttributeValueType.S)) ? cond.getAttributeValueList().get(0).getS() : cond.getAttributeValueList().get(0).getN();
									if (value.compareTo(comp) < 0) {
										items.add(item);
									}
								} else {
									//TODO to do
									//List<String> value = (getAttributeValueType(item.get(k)).equals(AttributeValueType.SS))? item.get(k).getSS() : item.get(k).getNS();
								}

							} else {
								//TODO to do
								if (item.get(k).equals(cond.getAttributeValueList())) {
									items.add(item);
								}
							}
						}
						if (cond.getComparisonOperator().equals("GE")) {
							if (cond.getAttributeValueList().size() == 1) {
								if (getAttributeValueType(item.get(k)).equals(AttributeValueType.S) || getAttributeValueType(item.get(k)).equals(AttributeValueType.N)) {
									String value = (getAttributeValueType(item.get(k)).equals(AttributeValueType.S)) ? item.get(k).getS() : item.get(k).getN();
									String comp = (getAttributeValueType(cond.getAttributeValueList().get(0)).equals(AttributeValueType.S)) ? cond.getAttributeValueList().get(0).getS() : cond.getAttributeValueList().get(0).getN();
									if (value.compareTo(comp) <= 0) {
										items.add(item);
									}
								} else {
									//TODO to do
									//List<String> value = (getAttributeValueType(item.get(k)).equals(AttributeValueType.SS))? item.get(k).getSS() : item.get(k).getNS();
								}

							} else {
								//TODO to do
								if (item.get(k).equals(cond.getAttributeValueList())) {
									items.add(item);
								}
							}
						}
						if (cond.getComparisonOperator().equals("GT")) {
							if (cond.getAttributeValueList().size() == 1) {
								if (getAttributeValueType(item.get(k)).equals(AttributeValueType.S) || getAttributeValueType(item.get(k)).equals(AttributeValueType.N)) {
									if (getAttributeValueType(item.get(k)).equals(AttributeValueType.S)) {
										String value = (getAttributeValueType(item.get(k)).equals(AttributeValueType.S)) ? item.get(k).getS() : item.get(k).getN();
										String comp = (getAttributeValueType(cond.getAttributeValueList().get(0)).equals(AttributeValueType.S)) ? cond.getAttributeValueList().get(0).getS() : cond.getAttributeValueList().get(0).getN();
										if (value.compareTo(comp) > 0) {
											items.add(item);
										}
									} else {
										String value = (getAttributeValueType(item.get(k)).equals(AttributeValueType.S)) ? item.get(k).getS() : item.get(k).getN();
										String comp = (getAttributeValueType(cond.getAttributeValueList().get(0)).equals(AttributeValueType.S)) ? cond.getAttributeValueList().get(0).getS() : cond.getAttributeValueList().get(0).getN();
										if (Integer.parseInt(value) > Integer.parseInt(comp)) {
											items.add(item);
										}
									}
								} else {
									//TODO to do
									//List<String> value = (getAttributeValueType(item.get(k)).equals(AttributeValueType.SS))? item.get(k).getSS() : item.get(k).getNS();
								}

							} else {
								//TODO to do
								if (item.get(k).equals(cond.getAttributeValueList())) {
									items.add(item);
								}
							}
						}
						if (cond.getComparisonOperator().equals("IN")) {
							for(AttributeValue value : cond.getAttributeValueList()){
								if(item.get(k).equals(value)){
									items.add(item);
								}
							}
						}
					}
				}
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

	public QueryResult query(QueryRequest request) {
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
				if (request.getScanIndexForward() == null || request.getScanIndexForward() == false) {
					list.add(0, getItemWithAttributesToGet(item, attributesToGet));
				} else {
					list.add(getItemWithAttributesToGet(item, attributesToGet));
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

	public UpdateItemResult updateItem(UpdateItemRequest request) {
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
		if (item == null) {
			if (request.getExpected()==null || request.getExpected().isEmpty()){
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
			}else{
				throw new ConditionalCheckFailedException("The value conditional could is not equal");
			}
		} else {
			if (isExpectedItem(item, request.getExpected())){			
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
								if (item.get(sKey).getSS() != null) {
									if (attributesToUpdate.get(sKey).getValue().getSS() == null) {
										throw new ConditionalCheckFailedException("It's not possible to delete something else than a List<String> for the attribute (" + sKey + ") of the item with hash key (" + item.get(sKey) + ")");
									} else {
										for (String toDel : attributesToUpdate.get(sKey).getValue().getSS()) {
											if (item.get(sKey).getSS().contains(toDel)) {
												item.get(sKey).getSS().remove(toDel);
												attributesToUpdate.remove(sKey);
											}
										}
									}
								} else if (item.get(sKey).getNS() != null) {
									if (attributesToUpdate.get(sKey).getValue().getNS() == null) {
										throw new ConditionalCheckFailedException("It's not possible to delete something else than a List<Number> for the attribute (" + sKey + ") of the item with hash key (" + item.get(sKey) + ")");
									} else {
										for (String toDel : attributesToUpdate.get(sKey).getValue().getNS()) {
											if (item.get(sKey).getNS().contains(toDel)) {
												item.get(sKey).getNS().remove(toDel);
												attributesToUpdate.remove(sKey);
											}
										}
									}
								} else if (item.get(sKey).getS().equals(attributesToUpdate.get(sKey).getValue().getS())) {
									item.remove(sKey);
									attributesToUpdate.remove(sKey);
								} else if (item.get(sKey).getN().equals(attributesToUpdate.get(sKey).getValue().getN())) {
									item.remove(sKey);
									attributesToUpdate.remove(sKey);
								}
							} else {
								item.remove(sKey);
								attributesToUpdate.remove(sKey);
							}
						} else if (attributesToUpdate.get(sKey).getAction().equalsIgnoreCase(AttributeAction.ADD.name())) {
							if (attributesToUpdate.get(sKey).getValue() != null) {
								if (item.get(sKey).getSS() != null) {
									if (attributesToUpdate.get(sKey).getValue().getSS() == null) {
										throw new ConditionalCheckFailedException("It's not possible to ADD something else than a List<String> for the attribute (" + sKey + ")");
									} else {
										for (String toUp : attributesToUpdate.get(sKey).getValue().getSS()) {
											item.get(sKey).getSS().add(toUp);
										}
									}
								} else if (item.get(sKey).getNS() != null) {
									if (attributesToUpdate.get(sKey).getValue().getNS() == null) {
										throw new ConditionalCheckFailedException("It's not possible to ADD something else than a List<Number> for the attribute (" + sKey + ")");
									} else {
										for (String toUp : attributesToUpdate.get(sKey).getValue().getNS()) {
											item.get(sKey).getNS().add(toUp);
										}
									}
								} else if (item.get(sKey).getS() != null) {
									throw new ConditionalCheckFailedException("It's not possible to ADD on an attribute with a String type for the attribute (" + sKey + ")");
								} else if (item.get(sKey).getN() != null) {
									Double i = new Double(item.get(sKey).getN());
									i = i + new Double(attributesToUpdate.get(sKey).getValue().getN());
									item.get(sKey).setN(i + "");
								}
							} else {
								throw new ResourceNotFoundException("the provided update item with attribute (" + sKey + ") doesn't have an AttributeValue to perform the ADD");
							}
						}
					}
				}
				
			}else{
				throw new ConditionalCheckFailedException("The value conditional could is not equal");
			}
			result.setAttributes(item);
		}
		return result;
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

	private boolean isExpectedItem(Map<String, AttributeValue> item, Map<String, ExpectedAttributeValue> expected){
		if (expected==null || expected.isEmpty()){
			return true;
		}
		for (String expectedFieldName: expected.keySet()){
			ExpectedAttributeValue expectedValue = expected.get(expectedFieldName);
			AttributeValue currentValue = item.get(expectedFieldName);
			if (currentValue==null) {
				//field is missing
				return false;
			}
			if (!expectedValue.getValue().equals(currentValue)){
				return false;
			}
		}
		return true;
	}
}

package com.michelboudreau.alternator;

import com.amazonaws.services.dynamodb.model.*;
import com.amazonaws.services.dynamodb.model.transform.*;
import com.michelboudreau.alternator.enums.AttributeValueType;
import com.michelboudreau.alternator.models.Limits;
import com.michelboudreau.alternator.models.Table;
import com.michelboudreau.alternator.parsers.AmazonWebServiceRequestParser;
import com.michelboudreau.alternator.validators.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class AlternatorDBHandler {

	private final Logger logger = LoggerFactory.getLogger(AlternatorDBHandler.class);
	private Map<String, Table> tables = new HashMap<String, Table>();
	private List<Table> tableList = new ArrayList<Table>();

	public AlternatorDBHandler() {
		// Should we save the results
		/*ObjectMapper mapper = new ObjectMapper();
		if (new File(dbName).exists()) {
			this.models = mapper.readValue(new File(dbName), AlternatorDB.class);
		}
		mapper.writeValue(new File(dbName), models);*/
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
			/*
													case UPDATE:
														return updateItem(parser.getData(UpdateItemRequest.class, UpdateItemRequestJsonUnmarshaller.getInstance()));
													case DELETE:
														return deleteItem(parser.getData(DeleteItemRequest.class, DeleteItemRequestJsonUnmarshaller.getInstance()));
													case BATCH_GET_ITEM:
														return batchGetItem(parser.getData(BatchGetItemRequest.class, BatchGetItemRequestJsonUnmarshaller.getInstance()));
													case BATCH_WRITE_ITEM:
														return batchWriteItem(parser.getData(BatchWriteItemRequest.class, BatchWriteItemRequestJsonUnmarshaller.getInstance()));

													// Operations
													case QUERY:
														return query(parser.getData(QueryRequest.class, QueryRequestJsonUnmarshaller.getInstance()));
													case SCAN:
														return scan(parser.getData(ScanRequest.class, ScanRequestJsonUnmarshaller.getInstance()));*/
			default:
				logger.warn("The Request Type '" + parser.getType() + "' does not exist.");
				break;
		}
		return null;
	}

	protected CreateTableResult createTable(CreateTableRequest request) throws LimitExceededException, InternalServerErrorException, ResourceInUseException {
		// table limit of 256
		if (this.tables.size() >= Limits.TABLE_MAX) {
			throw new LimitExceededException("Cannot exceed 256 tables per account.");
		}

		// Validate data coming in
		// TODO: Look into how we're doing validation, maybe implement better solution
		CreateTableRequestValidator validator = new CreateTableRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw createInternalServerEception(errors);
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

	protected DescribeTableResult describeTable(DescribeTableRequest request) throws InternalServerErrorException, ResourceNotFoundException {
		// Validate data coming in
		DescribeTableRequestValidator validator = new DescribeTableRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw createInternalServerEception(errors);
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

	protected ListTablesResult listTables(ListTablesRequest request) throws InternalServerErrorException, ResourceNotFoundException {
		// Validate data coming in
		ListTablesRequestValidator validator = new ListTablesRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw createInternalServerEception(errors);
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

	protected DeleteTableResult deleteTable(DeleteTableRequest request) throws InternalServerErrorException, ResourceNotFoundException {
		// Validate data coming in
		DeleteTableRequestValidator validator = new DeleteTableRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw createInternalServerEception(errors);
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

	protected UpdateTableResult updateTable(UpdateTableRequest request) throws InternalServerErrorException, ResourceNotFoundException {
		// Validate data coming in
		UpdateTableRequestValidator validator = new UpdateTableRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw createInternalServerEception(errors);
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

	protected PutItemResult putItem(PutItemRequest request) throws InternalServerErrorException, ResourceNotFoundException, ConditionalCheckFailedException {
		// Validate data coming in
		PutItemRequestValidator validator = new PutItemRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() != 0) {
			throw createInternalServerEception(errors);
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
		Map<String, AttributeValue> currentItem = table.getItem(getHashKeyName(request.getItem().get(table.getHashKeyName())));

		// Check conditional put
		if (request.getExpected() != null) {
			for (Map.Entry<String, ExpectedAttributeValue> entry : request.getExpected().entrySet()) {
				String key = entry.getKey();
				ExpectedAttributeValue value = entry.getValue();
				value.setExists(value.getValue() != null);
				if ((value.getExists() && currentItem == null) || (!value.getExists() && currentItem != null)) {
					throw new ConditionalCheckFailedException("The exist conditional could not be met.");
				}
				if (value.getValue() != null) {
					// check to see if value conditional is equal
					if (
							(value.getValue().getN() != null && !currentItem.get(key).equals(value.getValue().getN())) || (value.getValue().getS() != null && !currentItem.get(key).equals(value.getValue().getS())) || (value.getValue().getNS() != null && !currentItem.get(key).equals(value.getValue().getNS())) || (value.getValue().getSS() != null && !currentItem.get(key).equals(value.getValue().getSS()))
							) {
						throw new ConditionalCheckFailedException("The value conditional could is not equal");
					}
				}
			}
		}

		PutItemResult result = new PutItemResult().withConsumedCapacityUnits(1D);
		if (currentItem != null && request.getReturnValues() != null && ReturnValue.fromValue(request.getReturnValues()) == ReturnValue.ALL_OLD) {
			result.setAttributes(currentItem);
		}

		// put the item in the table
		table.putItem(request.getItem());

		return result;
	}

	protected GetItemResult getItem(GetItemRequest request) throws InternalServerErrorException, ResourceNotFoundException {
		String key = null;
		String tableName = null;
		Map<String, Object> response = new HashMap<String, Object>();
/*
		try {
			JsonNode actualObj = data.getData();
			tableName = actualObj.path("TableName").getTextValue();
			if (!actualObj.path("Key").path("HashKeyElement").path("S").isNull()) {
				key = actualObj.path("Key").path("HashKeyElement").path("S").getTextValue();
			} else if (!actualObj.path("Key").path("HashKeyElement").path("N").isNull()) {
				key = actualObj.path("Key").path("HashKeyElement").path("N").getTextValue();
			}

			if (key == null) {
				throw new IOException("Bad request in getItem");
			}


			if (getTable(tableName) == null) {
				throw new IOException("table doesn't exist");
			}

			response.put("ConsumedCapacityUnits", 1);
			List<Item> items = findItemByKey(key, tableName);
			if (items != null) {
				for (Item itm : items) {
					response.put("Item", itm.getAttributes());
				}
			}
		} catch (IOException e) {
			logger.debug("item wasn't put correctly : " + e);
		}*/
		return new GetItemResult();
	}

	protected Object updateItem(UpdateItemRequest request) {
		return new UpdateItemResult();
	}

	protected Object deleteItem(DeleteItemRequest request) {
		return new DeleteItemResult();
	}

	protected Object batchGetItem(BatchGetItemRequest request) {
		return new BatchGetItemResult();
	}

	protected Object batchWriteItem(BatchWriteItemRequest request) {
		return new BatchWriteItemResult();
	}

	protected Object scan(ScanRequest request) {
		/*List<HashMap<String, Map<String, String>>> result = new ArrayList<HashMap<String, Map<String, String>>>();
		Map<String, Object> map = new HashMap<String, Object>();
		JsonNode data = obj.getData();
		try {
			String tableName = data.path("TableName").getTextValue();
			String limit = null;
			if (!data.path("limit").isNull()) {
				limit = "" + data.path("limit").getIntValue();
			}
			if (data.path("ScanFilter").getTextValue() != null) {
				if (getTable(tableName).isHasRangeKey()) {
					String comparator = data.path("ScanFilter").path("ComparisonOperator").getTextValue();
					String rangeKey = tableGetRangeKey(tableName);
					String rangeKeyType = getTable(tableName).getRangeKeyType();
					if ("BETWEEN".equals(comparator)) {
						String lowerBound = data.path("ScanFilter").path(rangeKey).path("AttributeValueList").path(0).getTextValue();
						String upperBound = data.path("ScanFilter").path(rangeKey).path("AttributeValueList").path(1).getTextValue();
						for (Item itm : getTable(tableName).getItems()) {
							if ((lowerBound.compareTo(itm.getAttributes().get(itm.getRangeKey()).get(rangeKeyType)) < 0) && (upperBound.compareTo(itm.getAttributes().get(itm.getRangeKey()).get(rangeKeyType)) > 0)) {
								result.add(itm.getAttributes());
							}
						}
					}
					if ("LT".equals(comparator)) {
						String bound = data.path("ScanFilter").path(rangeKey).path("AttributeValueList").path(0).getTextValue();
						for (Item itm : getTable(tableName).getItems()) {
							if ((bound.compareTo(itm.getAttributes().get(itm.getRangeKey()).get(rangeKeyType)) > 0)) {
								result.add(itm.getAttributes());
							}
						}
					}
					if ("LE".equals(comparator)) {
						String bound = data.path("ScanFilter").path(rangeKey).path("AttributeValueList").path(0).getTextValue();
						for (Item itm : getTable(tableName).getItems()) {
							if ((bound.compareTo(itm.getAttributes().get(itm.getRangeKey()).get(rangeKeyType)) >= 0)) {
								result.add(itm.getAttributes());
							}
						}
					}
					if ("GT".equals(comparator)) {
						String bound = data.path("ScanFilter").path(rangeKey).path("AttributeValueList").path(0).getTextValue();
						for (Item itm : getTable(tableName).getItems()) {
							if ((bound.compareTo(itm.getAttributes().get(itm.getRangeKey()).get(rangeKeyType)) < 0)) {
								result.add(itm.getAttributes());
							}
						}
					}
					if ("GE".equals(comparator)) {
						String bound = data.path("ScanFilter").path(rangeKey).path("AttributeValueList").path(0).getTextValue();
						for (Item itm : getTable(tableName).getItems()) {
							if ((bound.compareTo(itm.getAttributes().get(itm.getRangeKey()).get(rangeKeyType)) <= 0)) {
								result.add(itm.getAttributes());
							}
						}
					}
				} else {
					throw new RuntimeException("RangeKeyCondition with no rangekey on the table");
				}
			} else {
				for (Item itm : getTable(tableName).getItems()) {
					result.add(itm.getAttributes());
				}

			}
			map.put("ConsumedCapacityUnits", 1);
			map.put("Count", 0);
			map.put("ScannedCount", 1);
			map.put("Items", result);

		} catch (RuntimeException e) {
			logger.debug("table wasn't created correctly : " + e);
		}
		System.out.println(map.toString());
		return map;*/
		return new ScanResult();
	}

	public Object query(QueryRequest request) {
		/*List<HashMap<String, Map<String, String>>> result = new ArrayList<HashMap<String, Map<String, String>>>();
		Map<String, Object> map = new HashMap<String, Object>();
		JsonNode data = obj.getData();
		try {
			String tableName = data.path("TableName").getTextValue();
			if (data.path("RangeKeyCondition").getTextValue() != null) {
				if (getTable(tableName).isHasRangeKey()) {
					String comparator = data.path("RangeKeyCondition").path("ComparisonOperator").getTextValue();
					String rangeKey = tableGetRangeKey(tableName);
					String rangeKeyType = getTable(tableName).getRangeKeyType();
					String hashKey = data.path("HashKeyValue").path(getTable(tableName).getHashKeyType()).getTextValue();
					if ("BETWEEN".equals(comparator)) {
						String lowerBound = data.path("RangeKeyCondition").path(rangeKey).path("AttributeValueList").path(0).getTextValue();
						String upperBound = data.path("RangeKeyCondition").path(rangeKey).path("AttributeValueList").path(1).getTextValue();
						for (Item itm : getTable(tableName).getItemsWithKey(hashKey)) {
							if ((lowerBound.compareTo(itm.getAttributes().get(itm.getRangeKey()).get(rangeKeyType)) < 0) && (upperBound.compareTo(itm.getAttributes().get(itm.getRangeKey()).get(rangeKeyType)) > 0)) {
								result.add(itm.getAttributes());
							}
						}
					}
					if ("LT".equals(comparator)) {
						String bound = data.path("RangeKeyCondition").path(rangeKey).path("AttributeValueList").path(0).getTextValue();
						for (Item itm : getTable(tableName).getItemsWithKey(hashKey)) {
							if ((bound.compareTo(itm.getAttributes().get(itm.getRangeKey()).get(rangeKeyType)) > 0)) {
								result.add(itm.getAttributes());
							}
						}
					}
					if ("LE".equals(comparator)) {
						String bound = data.path("RangeKeyCondition").path(rangeKey).path("AttributeValueList").path(0).getTextValue();
						for (Item itm : getTable(tableName).getItemsWithKey(hashKey)) {
							if ((bound.compareTo(itm.getAttributes().get(itm.getRangeKey()).get(rangeKeyType)) >= 0)) {
								result.add(itm.getAttributes());
							}
						}
					}
					if ("GT".equals(comparator)) {
						String bound = data.path("RangeKeyCondition").path(rangeKey).path("AttributeValueList").path(0).getTextValue();
						for (Item itm : getTable(tableName).getItemsWithKey(hashKey)) {
							if ((bound.compareTo(itm.getAttributes().get(itm.getRangeKey()).get(rangeKeyType)) < 0)) {
								result.add(itm.getAttributes());
							}
						}
					}
					if ("GE".equals(comparator)) {
						String bound = data.path("RangeKeyCondition").path(rangeKey).path("AttributeValueList").path(0).getTextValue();
						for (Item itm : getTable(tableName).getItemsWithKey(hashKey)) {
							if ((bound.compareTo(itm.getAttributes().get(itm.getRangeKey()).get(rangeKeyType)) <= 0)) {
								result.add(itm.getAttributes());
							}
						}
					}
				} else {
					throw new RuntimeException("RangeKeyCondition with no rangekey on the table");
				}
			} else {
				for (Item itm : getTable(tableName).getItems()) {
					result.add(itm.getAttributes());
				}

			}
			map.put("ConsumedCapacityUnits", 1);
			map.put("Count", 0);
			map.put("ScannedCount", 1);
			map.put("Items", result);

		} catch (RuntimeException e) {
			logger.debug("table wasn't created correctly : " + e);
		}
		System.out.println(map.toString());
		return map;*/
		return new QueryResult();
	}

	protected String getHashKeyName(AttributeValue value) {
		if (value.getN() != null) {
			return value.getN();
		} else if (value.getS() != null) {
			return value.getS();
		} /*else if (value.getNS() != null) {
			return AttributeValueType.NS;
		} else if (value.getSS() != null) {
			return AttributeValueType.SS;
		}*/
		return null;
	}

	protected AttributeValueType getAttributeValueType(AttributeValue value) {
		if (value.getN() != null) {
			return AttributeValueType.N;
		} else if (value.getS() != null) {
			return AttributeValueType.S;
		} else if (value.getNS() != null) {
			return AttributeValueType.NS;
		} else if (value.getSS() != null) {
			return AttributeValueType.SS;
		}
		return AttributeValueType.UNKNOWN;
	}

	protected InternalServerErrorException createInternalServerEception(List<Error> errors) {
		String message = "The following Errors occured: ";
		for (Error error : errors) {
			message += error.getMessage() + "\n";
		}
		return new InternalServerErrorException(message);
	}
}

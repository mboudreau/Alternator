package com.michelboudreau.alternator;

import com.amazonaws.services.dynamodb.model.*;
import com.amazonaws.services.dynamodb.model.transform.*;
import com.michelboudreau.alternator.models.Item;
import com.michelboudreau.alternator.models.Table;
import com.michelboudreau.alternator.parsers.AmazonWebServiceRequestParser;
import com.michelboudreau.alternator.validators.CreateTableRequestValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.*;

class AlternatorDBHandler {

	private final Logger logger = LoggerFactory.getLogger(AlternatorDBHandler.class);
	private Map<String, Table> tables = new HashMap<String, Table>();

	public AlternatorDBHandler() {
		// Should we save the results
		/*ObjectMapper mapper = new ObjectMapper();
		if (new File(dbName).exists()) {
			this.models = mapper.readValue(new File(dbName), AlternatorDB.class);
		}
		mapper.writeValue(new File(dbName), models);*/
	}

	public Object handle(HttpServletRequest request) {
		AmazonWebServiceRequestParser parser = new AmazonWebServiceRequestParser(request);

		switch (parser.getType()) {
			// Tables
			case CREATE_TABLE:
				return createTable(parser.getData(CreateTableRequest.class, CreateTableRequestJsonUnmarshaller.getInstance()));
			case DESCRIBE_TABLE:
				return describeTable(parser.getData(DescribeTableRequest.class, DescribeTableRequestJsonUnmarshaller.getInstance()));
			case LIST_TABLES:
				return listTables(parser.getData(ListTablesRequest.class, ListTablesRequestJsonUnmarshaller.getInstance()));
			case UPDATE_TABLE:
				return updateTable(parser.getData(UpdateTableRequest.class, UpdateTableRequestJsonUnmarshaller.getInstance()));
			case DELETE_TABLE:
				return deleteTable(parser.getData(DeleteTableRequest.class, DeleteTableRequestJsonUnmarshaller.getInstance()));

			// Items
			case PUT:
				return putItem(parser.getData(PutItemRequest.class, PutItemRequestJsonUnmarshaller.getInstance()));
			case GET:
				return getItem(parser.getData(GetItemRequest.class, GetItemRequestJsonUnmarshaller.getInstance()));
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
				return scan(parser.getData(ScanRequest.class, ScanRequestJsonUnmarshaller.getInstance()));
			default:
				logger.warn("The Request Type '" + parser.getType() + "' does not exist.");
				break;
		}
		return null;
	}

	protected Object createTable(CreateTableRequest request) {
		CreateTableRequestValidator validator = new CreateTableRequestValidator();
		List<Error> errors = validator.validate(request);
		if (errors.size() == 0) {
			// make sure request is filled properly
			String tableName = request.getTableName();
			KeySchemaElement hashKey = request.getKeySchema().getHashKeyElement();
			KeySchemaElement rangeKey = request.getKeySchema().getRangeKeyElement();
			String rangeKeyName = null;
			String rangeKeyType = null;
			if (rangeKey != null) {
				rangeKeyName = rangeKey.getAttributeName();
				rangeKeyType = rangeKey.getAttributeType();
			}

			if (!this.tables.containsKey(tableName)) {
				Table table = new Table(hashKey.getAttributeName(), rangeKeyName, tableName, hashKey.getAttributeType(), rangeKeyType);
				this.tables.put(tableName, table);
			} else {
				// TODO: create error
			}
			return new CreateTableResult();
		} else {
			return createInternalServerEception(errors);
		}
	}

	protected Object describeTable(DescribeTableRequest request) {
		return new DescribeTableResult();
	}

	protected Object listTables(ListTablesRequest request) {
		return new ListTablesResult();
	}

	protected Object updateTable(UpdateTableRequest request) {
		return new UpdateTableResult();
	}

	protected Object deleteTable(DeleteTableRequest request) {
		return new DeleteTableResult();
	}

	protected Object putItem(PutItemRequest request) {
		/*try {
			JsonNode actualObj = data.getData();
			String tableName = actualObj.path("TableName").getTextValue();
			Iterator itr = actualObj.path("Item").getFieldNames();
			HashMap<String, Map<String, String>> attributes = new HashMap<String, Map<String, String>>();
			while (itr.hasNext()) {
				String attrName = itr.next().toString();
				Map<String, String> schema = new HashMap<String, String>();
				if (actualObj.path("Item").path(attrName).path("S").getTextValue() != null) {
					schema.put("S", actualObj.path("Item").path(attrName).path("S").getTextValue());
					attributes.put(attrName, schema);
				} else if (actualObj.path("Item").path(attrName).path("N").getTextValue() != null) {
					schema.put("N", actualObj.path("Item").path(attrName).path("N").getTextValue());
					attributes.put(attrName, schema);
				}
			}
			if (getTable(tableName) == null) {
				throw new IOException("table doesn't exist");
			}
			if (findItemByAttributes(attributes, tableName) != null) {
				getTable(tableName).removeItem(findItemByAttributes(attributes, tableName));
			}
			if (attributes != null && !attributes.isEmpty()) {
				Item item = new Item(tableName, tableGetHashKey(tableName), tableGetRangeKey(tableName), attributes);
				getTable(tableName).addItem(item);
			} else {
				throw new IOException("item empty");
			}
		} catch (IOException e) {
			logger.debug("item wasn't put correctly : " + e);
		}*/
		return new PutItemResult();
	}

	protected Object getItem(GetItemRequest request) {
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

	protected InternalServerErrorException createInternalServerEception(List<Error> errors) {
		String message = "The following Errors occured: ";
		for (Error error : errors) {
			message += error.getMessage()+"\n";
		}
		return new InternalServerErrorException(message);
	}
}

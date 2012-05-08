package com.michelboudreau.alternator;

import com.amazonaws.services.dynamodb.model.*;
import com.amazonaws.services.dynamodb.model.transform.*;
import com.michelboudreau.alternator.models.Item;
import com.michelboudreau.alternator.models.Table;
import com.michelboudreau.alternator.parsers.AmazonWebServiceRequestParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.*;

class AlternatorDBHandler {

	private final Logger logger = LoggerFactory.getLogger(AlternatorDBHandler.class);
	private List<Table> tables = new ArrayList<Table>();

	public AlternatorDBHandler() {
		// Should we save the results
		/*ObjectMapper mapper = new ObjectMapper();
		if (new File(dbName).exists()) {
			this.models = mapper.readValue(new File(dbName), AlternatorDB.class);
		}
		mapper.writeValue(new File(dbName), models);*/
	}

	public Object handle(HttpServletRequest request) throws ConditionalCheckFailedException, InternalServerErrorException, ResourceInUseException, ResourceNotFoundException {
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

	protected CreateTableResult createTable(CreateTableRequest request) throws InternalServerErrorException {
		String tableName = request.getTableName();
		KeySchemaElement hashKey = request.getKeySchema().getHashKeyElement();
		KeySchemaElement rangeKey = request.getKeySchema().getRangeKeyElement();
		// TODO: make sure request is filled properly
		if (getTable(tableName) == null) {
			Table table = new Table(hashKey.getAttributeName(), rangeKey.getAttributeName(), tableName, hashKey.getAttributeType(), rangeKey.getAttributeType());
			this.tables.add(table);
		} else {
			// TODO: create error
		}

		return new CreateTableResult();
	}

	protected DescribeTableResult describeTable(DescribeTableRequest request) {
		return new DescribeTableResult();
	}

	protected ListTablesResult listTables(ListTablesRequest request) {
		return new ListTablesResult();
	}

	protected UpdateTableResult updateTable(UpdateTableRequest request) {
		return new UpdateTableResult();
	}

	protected DeleteTableResult deleteTable(DeleteTableRequest request) {
		return new DeleteTableResult();
	}

	protected PutItemResult putItem(PutItemRequest request) {
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

	protected GetItemResult getItem(GetItemRequest request) {
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

	protected UpdateItemResult updateItem(UpdateItemRequest request) {
		return null;
	}

	protected DeleteItemResult deleteItem(DeleteItemRequest request) {
		return null;
	}

	protected BatchGetItemResult batchGetItem(BatchGetItemRequest request) {
		return new BatchGetItemResult();
	}

	protected BatchWriteItemResult batchWriteItem(BatchWriteItemRequest request) {
		return new BatchWriteItemResult();
	}

	protected ScanResult scan(ScanRequest request) {
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

	public QueryResult query(QueryRequest request) {
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

	protected Table getTable(String tableName) {
		for (Table table : this.tables) {
			if (tableName.equals(table.getName())) {
				return table;
			}
		}
		return null;
	}

	protected Item findItemByAttributes(HashMap<String, Map<String, String>> attr, String tableName) {
		Item result = null;
		for (Table table : this.tables) {
			if (tableName.equals(table.getName())) {
				if (table.getItems() != null) {
					for (Item itm : table.getItems()) {
						if (valuesFromAttributes(attr).equals(valuesFromAttributes(itm.getAttributes()))) {
							result = itm;
						}
					}
				}
			}
		}
		return result;
	}

	protected List<Item> findItemByKey(String key, String tableName) throws IOException {
		List<Item> result = new ArrayList<Item>();
		for (Table table : this.tables) {
			if (tableName.equals(table.getName())) {
				if (table.getItems() != null) {
					for (Item itm : table.getItems()) {
						if (key != null) {
							if (!itm.getAttributes().get(table.getHashKey()).get("S").isEmpty()) {
								if (key.equals(itm.getAttributes().get(table.getHashKey()).get("S"))) {
									result.add(itm);
								}
							} else if (!itm.getAttributes().get(table.getHashKey()).get("N").isEmpty()) {
								if (key.equals(itm.getAttributes().get(table.getHashKey()).get("N"))) {
									result.add(itm);
								}
							}
						} else {
							throw new IOException("bad requests in findItemByKey");
						}
					}
				}
			}
		}
		return result;
	}

	protected HashSet<String> valuesFromAttributes(HashMap<String, Map<String, String>> map) {
		HashSet<String> result = new HashSet<String>();
		for (Map<String, String> mp : map.values()) {
			for (String attr : mp.values()) {
				result.add(attr);
			}
		}
		return result;
	}
}

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
import java.util.*;

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

            case UPDATE:
                return new UpdateItemResultMarshaller().marshall(updateItem(parser.getData(UpdateItemRequest.class, UpdateItemRequestJsonUnmarshaller.getInstance())));
            case DELETE:
                return new DeleteItemResultMarshaller().marshall(deleteItem(parser.getData(DeleteItemRequest.class, DeleteItemRequestJsonUnmarshaller.getInstance())));
            /*
                                                                                  case BATCH_GET_ITEM:
                                                                                      return batchGetItem(parser.getData(BatchGetItemRequest.class, BatchGetItemRequestJsonUnmarshaller.getInstance()));
                                                                                  case BATCH_WRITE_ITEM:
                                                                                      return batchWriteItem(parser.getData(BatchWriteItemRequest.class, BatchWriteItemRequestJsonUnmarshaller.getInstance()));
                                                                                      */
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
            throw createInternalServerException(errors);
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
            throw createInternalServerException(errors);
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

    protected DeleteTableResult deleteTable(DeleteTableRequest request) throws InternalServerErrorException, ResourceNotFoundException {
        // Validate data coming in
        DeleteTableRequestValidator validator = new DeleteTableRequestValidator();
        List<Error> errors = validator.validate(request);
        if (errors.size() != 0) {
            throw createInternalServerException(errors);
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
            throw createInternalServerException(errors);
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
            throw createInternalServerException(errors);
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
        Map<String, AttributeValue> item = table.getItem(getKeyValue(request.getItem().get(table.getHashKeyName())));

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

    protected GetItemResult getItem(GetItemRequest request) throws InternalServerErrorException, ResourceNotFoundException {
        // Validate data coming in
        GetItemRequestValidator validator = new GetItemRequestValidator();
        List<Error> errors = validator.validate(request);
        if (errors.size() != 0) {
            throw createInternalServerException(errors);
        }

        // get information
        String tableName = request.getTableName();
        Key key = request.getKey();
        List<String> attributesToGet = request.getAttributesToGet();
        Map<String, AttributeValue> response = new HashMap<String, AttributeValue>();
        GetItemResult result = new GetItemResult();

        // Check to make sure table exists
        if (!this.tables.containsKey(tableName)) {
            throw new ResourceNotFoundException("The table you're currently trying to access (" + tableName + ") doesn't exists.");
        }
        // Check to make sure Key is valid
        String keyz = "";
        if (key.getHashKeyElement().getS() != null) {
            keyz = key.getHashKeyElement().getS();
        } else if (key.getHashKeyElement().getN() != null) {
            keyz = key.getHashKeyElement().getN();
        }
        if (this.tables.get(tableName).getItem(keyz) == null) {
            throw new ResourceNotFoundException("The item with Hash Key (" + getKeyValue(key.getHashKeyElement()) + ") you try to get doesn't exists.");
        } else {
            if (attributesToGet == null) {
                result.setItem(this.tables.get(tableName).getItem(keyz));
            } else {
                for (String att : attributesToGet) {
                    AttributeValue res = this.tables.get(tableName).getItem(getKeyValue(key.getHashKeyElement())).get(att);
                    if (res != null) {
                        response.put(att, res);
                    }
                }
                result.setItem(response);
            }
        }
        return result;
    }

    protected DeleteItemResult deleteItem(DeleteItemRequest request) {
        // Validate data coming in
        DeleteItemRequestValidator validator = new DeleteItemRequestValidator();
        List<Error> errors = validator.validate(request);
        if (errors.size() != 0) {
            throw createInternalServerException(errors);
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
        Map<String, AttributeValue> item = table.getItem(hashKey);

        if (item == null) {
            throw new ResourceNotFoundException("The item with hash key '" + hashKey + "' doesn't exist in table '" + table.getName() + "'");
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
        table.removeItem(hashKey);
        return result;
    }

    protected Object batchGetItem(BatchGetItemRequest request) {
        return new BatchGetItemResult();
    }

    protected Object batchWriteItem(BatchWriteItemRequest request) {
        return new BatchWriteItemResult();
    }

    protected ScanResult scan(ScanRequest request) {
        ScanResult result = new ScanResult();
        List<Error> errors = new ScanRequestValidator().validate(request);
        if (errors.size() > 0) {
            throw createInternalServerException(errors);
        }
        result.setConsumedCapacityUnits(0.5);
        result.setLastEvaluatedKey(new Key());
        List<Map<String, AttributeValue>> items = new ArrayList<Map<String, AttributeValue>>();
        for (String key : this.tables.get(request.getTableName()).getItems().keySet()) {
            Map<String, AttributeValue> item = this.tables.get(request.getTableName()).getItem(key);
            if (request.getScanFilter() != null) {
                for (String k : request.getScanFilter().keySet()) {
                    Condition cond = request.getScanFilter().get(k);
                    if(cond.getComparisonOperator()==null){
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
                                String value = (getAttributeValueType(item.get(k)).equals(AttributeValueType.S)) ? item.get(k).getS() : item.get(k).getN();
                                String comp = (getAttributeValueType(cond.getAttributeValueList().get(0)).equals(AttributeValueType.S)) ? cond.getAttributeValueList().get(0).getS() : cond.getAttributeValueList().get(0).getN();
                                if (value.compareTo(comp) > 0) {
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
                }
            } else {
                items.add(item);
            }
        }
        if (request.getLimit() != null) {
            items = items.subList(0, request.getLimit() - 1);
        }

        for (Map<String, AttributeValue> item : items) {
            result.setLastEvaluatedKey(new Key(item.get(this.tables.get(request.getTableName()).getHashKeyName())));
        }

        if (request.getAttributesToGet() != null) {
            List<Map<String,AttributeValue>> copy = new ArrayList<Map<String, AttributeValue>>();
            for (Map<String, AttributeValue> item : items) {
                Set<String> keyz = new HashSet<String>(item.keySet());
                for(String sKey : keyz){
                   if(!request.getAttributesToGet().contains(sKey)){
                       item.remove(sKey);
                   }
                }
                copy.add(item);
            }
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

	    String keyValue = getKeyValue(request.getHashKeyValue());

	    // Check existence of table
	    Table table = this.tables.get(request.getTableName());
	    if (table == null) {
		    throw new ResourceNotFoundException("The table '" + request.getTableName() + "' doesn't exist.");
	    }

	    Map<String,AttributeValue> item = table.getItem(keyValue);

	    QueryResult queryResult = new QueryResult();
	    List<Map<String,AttributeValue>> list = new ArrayList<Map<String, AttributeValue>>();
	    list.add(item);
		queryResult.setItems(list);
		queryResult.setCount(list.size());
	    queryResult.setConsumedCapacityUnits(0.5);
	    queryResult.setLastEvaluatedKey(new Key(request.getHashKeyValue()));

        return queryResult;
    }

    protected String getKeyValue(AttributeValue value) {
        if (value != null) {
            if (value.getN() != null) {
                return value.getN();
            } else if (value.getS() != null) {
                return value.getS();
            }
        }
        return null;
    }

    protected AttributeValueType getAttributeValueType(AttributeValue value) {
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

    protected InternalServerErrorException createInternalServerException(List<Error> errors) {
        String message = "The following Errors occured: ";
        for (Error error : errors) {
            message += error.getMessage() + "\n";
        }
        return new InternalServerErrorException(message);
    }


    protected UpdateItemResult updateItem(UpdateItemRequest request) {
        // Validate data coming in
        // TODO: Look into how we're doing validation, maybe implement better solution
        UpdateItemRequestValidator validator = new UpdateItemRequestValidator();
        List<Error> errors = validator.validate(request);
        if (errors.size() != 0) {
            throw createInternalServerException(errors);
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
        if (this.tables.get(tableName).getItem(getKeyValue(key.getHashKeyElement())) == null) {
            Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
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
            Map<String, AttributeValue> item = this.tables.get(tableName).getItem(getKeyValue(key.getHashKeyElement()));
            Set<String> sKeyz = new HashSet<String>(item.keySet());
            for (String sKey : sKeyz) {
                if (attributesToUpdate.containsKey(sKey)) {
                    if (attributesToUpdate.get(sKey).getAction().equals("PUT")) {
                        item.remove(sKey);
                        item.put(sKey, attributesToUpdate.get(sKey).getValue());
                        attributesToUpdate.remove(sKey);
                    } else if (attributesToUpdate.get(sKey).getAction().equals("DELETE")) {
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
                    } else if (attributesToUpdate.get(sKey).getAction().equals("ADD")) {
                        if (attributesToUpdate.get(sKey).getValue() != null) {
                            if (item.get(sKey).getSS() != null) {
                                if (attributesToUpdate.get(sKey).getValue().getSS() == null) {
                                    throw new ConditionalCheckFailedException("It's not possible to delete something else than a List<String> for the attribute (" + sKey + ")");
                                } else {
                                    for (String toUp : attributesToUpdate.get(sKey).getValue().getSS()) {
                                        item.get(sKey).getSS().add(toUp);
                                    }
                                }
                            } else if (item.get(sKey).getNS() != null) {
                                if (attributesToUpdate.get(sKey).getValue().getNS() == null) {
                                    throw new ConditionalCheckFailedException("It's not possible to delete something else than a List<Number> for the attribute (" + sKey + ")");
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
            for(String sKey : attributesToUpdate.keySet())   {
                item.put(sKey,attributesToUpdate.get(sKey).getValue());
            }
            result.setAttributes(item);
        }
        return result;
    }


}

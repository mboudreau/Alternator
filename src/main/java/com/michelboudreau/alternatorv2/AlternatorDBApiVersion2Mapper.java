package com.michelboudreau.alternatorv2;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodb.model.BatchGetItemResult;
import com.amazonaws.services.dynamodb.model.BatchResponse;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.CreateTableResult;
import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemResult;
import com.amazonaws.services.dynamodb.model.DeleteTableRequest;
import com.amazonaws.services.dynamodb.model.DeleteTableResult;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableResult;
import com.amazonaws.services.dynamodb.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.services.dynamodb.model.GetItemResult;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.KeysAndAttributes;
import com.amazonaws.services.dynamodb.model.ListTablesRequest;
import com.amazonaws.services.dynamodb.model.ListTablesResult;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.amazonaws.services.dynamodb.model.UpdateItemResult;
import com.amazonaws.services.dynamodb.model.UpdateTableRequest;
import com.amazonaws.services.dynamodb.model.UpdateTableResult;
import com.michelboudreau.alternator.models.Table;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlternatorDBApiVersion2Mapper
{
    public static Map<String, AttributeValue> MapV2ItemToV1(
            Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> v2Item) {

        Map<String, AttributeValue> v1Item = null;
        if (v2Item != null) {
            v1Item = new HashMap<String, AttributeValue>();
            for (String key : v2Item.keySet()) {
                com.amazonaws.services.dynamodbv2.model.AttributeValue v2Value = v2Item.get(key);
                AttributeValue v1Value = MapV2AttributeValueToV1(v2Value);
                v1Item.put(key, v1Value);
            }
        }
        return v1Item;
    }

    public static Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> MapV1ItemToV2(
            Map<String, AttributeValue> v1Item) {

        Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> v2Item = null;
        if (v1Item != null) {
            v2Item = new HashMap<String, com.amazonaws.services.dynamodbv2.model.AttributeValue>();
            for (String key : v1Item.keySet()) {
                AttributeValue value = v1Item.get(key);
                com.amazonaws.services.dynamodbv2.model.AttributeValue v2Value = MapV1AttributeValueToV2(value);
                v2Item.put(key, v2Value);
            }
        }
        return v2Item;
    }

    public static Map<String, ExpectedAttributeValue> MapV2ExpectedToV1(
            Map<String, com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue> v2Expected) {

        Map<String, ExpectedAttributeValue> v1Expected = null;
        if (v2Expected != null) {
            v1Expected = new HashMap<String, ExpectedAttributeValue>();
            for (String key : v2Expected.keySet()) {
                com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue v2ExpValue = v2Expected.get(key);
                ExpectedAttributeValue v1ExpValue = MapV2ExpectedAttributeValueToV1(v2ExpValue);
                v1Expected.put(key, v1ExpValue);
            }
        }
        return v1Expected;
    }

    public static AttributeValue MapV2AttributeValueToV1(
            com.amazonaws.services.dynamodbv2.model.AttributeValue v2Value) {

        AttributeValue v1Value = null;
        if (v2Value != null) {
            v1Value =
                new AttributeValue()
                    .withB(v2Value.getB())
                    .withBS(v2Value.getBS())
                    .withN(v2Value.getN())
                    .withNS(v2Value.getNS())
                    .withS(v2Value.getS())
                    .withSS(v2Value.getSS())
                    ;
        }
        return v1Value;
    }

    public static com.amazonaws.services.dynamodbv2.model.AttributeValue MapV1AttributeValueToV2(
            AttributeValue v1Value) {

        com.amazonaws.services.dynamodbv2.model.AttributeValue v2Value =
            new com.amazonaws.services.dynamodbv2.model.AttributeValue()
                .withB(v1Value.getB())
                .withBS(v1Value.getBS())
                .withN(v1Value.getN())
                .withNS(v1Value.getNS())
                .withS(v1Value.getS())
                .withSS(v1Value.getSS())
                ;
        return v2Value;
    }

    public static ExpectedAttributeValue MapV2ExpectedAttributeValueToV1(
            com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue v2Expected) {

        ExpectedAttributeValue v1Expected =
            new ExpectedAttributeValue()
                .withExists(v2Expected.getExists())
                .withValue(MapV2AttributeValueToV1(v2Expected.getValue()))
                ;
        return v1Expected;
    }

    public static AttributeValueUpdate MapV2AttributeValueUpdateToV1(
            com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate v2AttrUpdate) {

        AttributeValueUpdate v1AttrUpdate =
            new AttributeValueUpdate()
                .withValue(MapV2AttributeValueToV1(v2AttrUpdate.getValue()))
                .withAction(v2AttrUpdate.getAction())
                ;
        return v1AttrUpdate;
    }

    public static Map<String, AttributeValueUpdate> MapV2AttributeUpdatesToV1(
            Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate> v2AttrUpdates) {

        Map<String, AttributeValueUpdate> v1AttrUpdates = null;
        if (v2AttrUpdates != null) {
            v1AttrUpdates = new HashMap<String, AttributeValueUpdate>();
            for (String key : v2AttrUpdates.keySet()) {
                com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate v2AttrUpdate = v2AttrUpdates.get(key);
                AttributeValueUpdate v1AttrUpdate = MapV2AttributeValueUpdateToV1(v2AttrUpdate);
                v1AttrUpdates.put(key, v1AttrUpdate);
            }
        }
        return v1AttrUpdates;
    }

    public static Key MapV2KeyToV1(
            Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> v2Key,
            Table table) {

        Key v1Key = null;

        if ((v2Key != null) && (table != null)) {
            v1Key = new Key();
            for (String attrName : v2Key.keySet()) {
                com.amazonaws.services.dynamodbv2.model.AttributeValue v2AttrValue = v2Key.get(attrName);
                if (attrName.equals(table.getHashKeyName())) {
                    v1Key.setHashKeyElement(MapV2AttributeValueToV1(v2AttrValue));
                } else if (attrName.equals(table.getRangeKeyName())) {
                    v1Key.setRangeKeyElement(MapV2AttributeValueToV1(v2AttrValue));
                }
            }
        }

        return v1Key;
    }

    public static Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> MapV1KeyToV2(
            Key v1Key,
            Table table) {

        Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> v2Key = null;
        if (v1Key != null) {
            v2Key = new HashMap<String, com.amazonaws.services.dynamodbv2.model.AttributeValue>();
            if (v1Key.getHashKeyElement() != null) {
                v2Key.put(table.getHashKeyName(), MapV1AttributeValueToV2(v1Key.getHashKeyElement()));
            }
            if (v1Key.getRangeKeyElement() != null) {
                v2Key.put(table.getRangeKeyName(), MapV1AttributeValueToV2(v1Key.getRangeKeyElement()));
            }
        }

        return v2Key;
    }

    public static KeysAndAttributes MapV2KeysAndAttributesToV1(
            com.amazonaws.services.dynamodbv2.model.KeysAndAttributes v2KeysAndAttr,
            Table table) {

        List<Key> v1Keys = new ArrayList<Key>();
        for (Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> v2Key : v2KeysAndAttr.getKeys()) {
            v1Keys.add(MapV2KeyToV1(v2Key, table));
        }
        KeysAndAttributes v1KeysAndAttr =
            new KeysAndAttributes()
                .withAttributesToGet(v2KeysAndAttr.getAttributesToGet())
                .withConsistentRead(v2KeysAndAttr.getConsistentRead())
                .withKeys(v1Keys)
                ;
        return v1KeysAndAttr;
    }

    public static com.amazonaws.services.dynamodbv2.model.KeysAndAttributes MapV1KeysAndAttributesToV2(
            KeysAndAttributes v1KeysAndAttr,
            Table table) {

        List<Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue>> v2Keys =
                new ArrayList<Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue>>();
        for (Key v1Key : v1KeysAndAttr.getKeys()) {
            v2Keys.add(MapV1KeyToV2(v1Key, table));
        }
        com.amazonaws.services.dynamodbv2.model.KeysAndAttributes v2KeysAndAttr =
            new com.amazonaws.services.dynamodbv2.model.KeysAndAttributes()
                .withAttributesToGet(v1KeysAndAttr.getAttributesToGet())
                .withConsistentRead(v1KeysAndAttr.getConsistentRead())
                .withKeys(v2Keys)
                ;
        return v2KeysAndAttr;
    }

    public static com.amazonaws.services.dynamodbv2.model.TableDescription MapV2TableDescriptionToV1(
            TableDescription table) {

        List<com.amazonaws.services.dynamodbv2.model.AttributeDefinition> v2Attributes =
            new ArrayList<com.amazonaws.services.dynamodbv2.model.AttributeDefinition>();
        List<com.amazonaws.services.dynamodbv2.model.KeySchemaElement> v2KeySchema =
            new ArrayList<com.amazonaws.services.dynamodbv2.model.KeySchemaElement>();
        KeySchema keySchema = table.getKeySchema();
        if (keySchema != null) {
            if (keySchema.getHashKeyElement() != null) {
                v2Attributes.add(
                    new com.amazonaws.services.dynamodbv2.model.AttributeDefinition()
                        .withAttributeName(keySchema.getHashKeyElement().getAttributeName())
                        .withAttributeType(keySchema.getHashKeyElement().getAttributeType())
                );
                v2KeySchema.add(
                    new com.amazonaws.services.dynamodbv2.model.KeySchemaElement()
                        .withAttributeName(keySchema.getHashKeyElement().getAttributeName())
                        .withKeyType("HASH")
                );
            }
            if (keySchema.getRangeKeyElement() != null) {
                v2Attributes.add(
                    new com.amazonaws.services.dynamodbv2.model.AttributeDefinition()
                        .withAttributeName(keySchema.getRangeKeyElement().getAttributeName())
                        .withAttributeType(keySchema.getRangeKeyElement().getAttributeType())
                );
                v2KeySchema.add(
                    new com.amazonaws.services.dynamodbv2.model.KeySchemaElement()
                        .withAttributeName(keySchema.getRangeKeyElement().getAttributeName())
                        .withKeyType("RANGE")
                );
            }
        }

        com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription v2ThruPut = null;
        ProvisionedThroughputDescription v1ThruPut = table.getProvisionedThroughput();
        if (v1ThruPut != null) {
            v2ThruPut =
                new com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription()
                    .withReadCapacityUnits(v1ThruPut.getReadCapacityUnits())
                    .withWriteCapacityUnits(v1ThruPut.getWriteCapacityUnits())
                    .withLastDecreaseDateTime(v1ThruPut.getLastDecreaseDateTime())
                    .withLastIncreaseDateTime(v1ThruPut.getLastIncreaseDateTime())
                    ;
        }

        com.amazonaws.services.dynamodbv2.model.TableDescription v2Table =
            new com.amazonaws.services.dynamodbv2.model.TableDescription()
                .withTableName(table.getTableName())
                .withTableStatus(table.getTableStatus())
                .withAttributeDefinitions(v2Attributes)
                .withKeySchema(v2KeySchema)
                .withCreationDateTime(table.getCreationDateTime())
                .withProvisionedThroughput(v2ThruPut)
                ;
        return v2Table;
    }

    public static Map<String, Condition> MapV2ScanFilterToV1(
            Map<String, com.amazonaws.services.dynamodbv2.model.Condition> v2Filter) {

        Map<String, Condition> v1Filter = null;
        if (v2Filter != null) {
            v1Filter = new HashMap<String, Condition>();
            for (String key : v2Filter.keySet()) {
                com.amazonaws.services.dynamodbv2.model.Condition v2Condition = v2Filter.get(key);
                Condition v1Condition = MapV2ConditionToV1(v2Condition);
                v1Filter.put(key, v1Condition);
            }
        }
        return v1Filter;
    }

    public static Condition MapV2ConditionToV1(
            com.amazonaws.services.dynamodbv2.model.Condition v2Condition) {

        Condition v1Condition =
            new Condition()
                .withComparisonOperator(v2Condition.getComparisonOperator())
                ;
        List<AttributeValue> v1AttrValues = null;
        List<com.amazonaws.services.dynamodbv2.model.AttributeValue> v2AttrValues = v2Condition.getAttributeValueList();
        if (v2AttrValues != null) {
            v1AttrValues = new ArrayList<AttributeValue>();
            for (com.amazonaws.services.dynamodbv2.model.AttributeValue v2AttrValue : v2AttrValues) {
                v1AttrValues.add(MapV2AttributeValueToV1(v2AttrValue));
            }
        }
        v1Condition.setAttributeValueList(v1AttrValues);
        return v1Condition;
    }

    public static CreateTableRequest MapV2CreateTableRequestToV1(
            com.amazonaws.services.dynamodbv2.model.CreateTableRequest v2Request) {

        String hashKeyName = null;
        String hashKeyType = null;

        String rangeKeyName = null;
        String rangeKeyType = null;

        for (com.amazonaws.services.dynamodbv2.model.KeySchemaElement element : v2Request.getKeySchema()) {
            if ("HASH".equals(element.getKeyType())) {
                hashKeyName = element.getAttributeName();
            } else if ("RANGE".equals(element.getKeyType())) {
                rangeKeyName = element.getAttributeName();
            }
        }

        for (com.amazonaws.services.dynamodbv2.model.AttributeDefinition attribute : v2Request.getAttributeDefinitions()) {
            if (attribute.getAttributeName().equals(hashKeyName)) {
                hashKeyType = attribute.getAttributeType();
            } else if (attribute.getAttributeName().equals(rangeKeyName)) {
                rangeKeyType = attribute.getAttributeType();
            }
        }

        KeySchema keySchema = new KeySchema();
        if (hashKeyName != null) {
            keySchema.setHashKeyElement(
                new KeySchemaElement().withAttributeName(hashKeyName).withAttributeType(hashKeyType)
            );
        }
        if (rangeKeyName != null) {
            keySchema.setRangeKeyElement(
                new KeySchemaElement().withAttributeName(rangeKeyName).withAttributeType(rangeKeyType)
            );
        }

        ProvisionedThroughput v1ThruPut = null;
        com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput v2ThruPut = v2Request.getProvisionedThroughput();
        if (v2ThruPut != null) {
            v1ThruPut =
                new ProvisionedThroughput()
                    .withReadCapacityUnits(v2ThruPut.getReadCapacityUnits())
                    .withWriteCapacityUnits(v2ThruPut.getWriteCapacityUnits())
                    ;
        }

        CreateTableRequest request =
            new CreateTableRequest()
                .withTableName(v2Request.getTableName())
                .withKeySchema(keySchema)
                .withProvisionedThroughput(v1ThruPut)
                ;

        return request;
    }

    public static DescribeTableRequest MapV2DescribeTableRequestToV1(
            com.amazonaws.services.dynamodbv2.model.DescribeTableRequest v2Request) {

        DescribeTableRequest request =
            new DescribeTableRequest()
                .withTableName(v2Request.getTableName())
                ;
        return request;
    }

    public static ListTablesRequest MapV2ListTablesRequestToV1(
            com.amazonaws.services.dynamodbv2.model.ListTablesRequest v2Request) {

        ListTablesRequest request =
            new ListTablesRequest()
                .withExclusiveStartTableName(v2Request.getExclusiveStartTableName())
                .withLimit(v2Request.getLimit())
                ;
        return request;

    }

    public static DeleteTableRequest MapV2DeleteTableRequestToV1(
            com.amazonaws.services.dynamodbv2.model.DeleteTableRequest v2Request) {

        DeleteTableRequest request =
            new DeleteTableRequest()
                .withTableName(v2Request.getTableName())
                ;
        return request;
    }

    public static UpdateTableRequest MapV2UpdateTableRequestToV1(
            com.amazonaws.services.dynamodbv2.model.UpdateTableRequest v2Request) {

        ProvisionedThroughput v1ThruPut = null;
        if (v2Request.getProvisionedThroughput() != null) {
            com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput v2ThruPut = v2Request.getProvisionedThroughput();
            v1ThruPut =
                new ProvisionedThroughput()
                    .withReadCapacityUnits(v2ThruPut.getReadCapacityUnits())
                    .withWriteCapacityUnits(v2ThruPut.getWriteCapacityUnits())
                    ;
        }
        UpdateTableRequest request =
            new UpdateTableRequest()
                .withTableName(v2Request.getTableName())
                .withProvisionedThroughput(v1ThruPut)
                ;

        return request;
    }

    public static PutItemRequest MapV2PutItemRequestToV1(
            com.amazonaws.services.dynamodbv2.model.PutItemRequest v2Request) {

        PutItemRequest request =
            new PutItemRequest()
                .withTableName(v2Request.getTableName())
                .withItem(MapV2ItemToV1(v2Request.getItem()))
                .withExpected(MapV2ExpectedToV1(v2Request.getExpected()))
                .withReturnValues(v2Request.getReturnValues())
                ;
        return request;
    }

    public static GetItemRequest MapV2GetItemRequestToV1(
            com.amazonaws.services.dynamodbv2.model.GetItemRequest v2Request,
            Table table) {

        GetItemRequest request =
            new GetItemRequest()
                .withTableName(v2Request.getTableName())
                .withKey(MapV2KeyToV1(v2Request.getKey(), table))
                .withAttributesToGet(v2Request.getAttributesToGet())
                .withConsistentRead(v2Request.getConsistentRead())
                ;
        return request;
    }

    public static DeleteItemRequest MapV2DeleteItemRequestToV1(
            com.amazonaws.services.dynamodbv2.model.DeleteItemRequest v2Request,
            Table table) {

        DeleteItemRequest request =
            new DeleteItemRequest()
                .withTableName(v2Request.getTableName())
                .withKey(MapV2KeyToV1(v2Request.getKey(), table))
                .withExpected(MapV2ExpectedToV1(v2Request.getExpected()))
                .withReturnValues(v2Request.getReturnValues())
                ;
        return request;
    }

    public static UpdateItemRequest MapV2UpdateItemRequestToV1(
            com.amazonaws.services.dynamodbv2.model.UpdateItemRequest v2Request,
            Table table) {

        UpdateItemRequest request =
            new UpdateItemRequest()
                .withTableName(v2Request.getTableName())
                .withKey(MapV2KeyToV1(v2Request.getKey(), table))
                .withAttributeUpdates(MapV2AttributeUpdatesToV1(v2Request.getAttributeUpdates()))
                .withExpected(MapV2ExpectedToV1(v2Request.getExpected()))
                .withReturnValues(v2Request.getReturnValues())
                ;
        return request;
    }

    public static BatchGetItemRequest MapV2BatchGetItemRequestToV1(
            com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest v2Request,
            Map<String, Table> tables) {

        Map<String, KeysAndAttributes> v1RequestItems = new HashMap<String, KeysAndAttributes>();
        for (String tableName : v2Request.getRequestItems().keySet()) {
            Table table = tables.get(tableName);
            KeysAndAttributes v1RequestItem = MapV2KeysAndAttributesToV1(v2Request.getRequestItems().get(tableName), table);
            v1RequestItems.put(tableName, v1RequestItem);
        }

        BatchGetItemRequest request =
            new BatchGetItemRequest()
                .withRequestItems(v1RequestItems)
                ;
        return request;
    }

    public static ScanRequest MapV2ScanRequestToV1(
            com.amazonaws.services.dynamodbv2.model.ScanRequest v2Request,
            Table table) {

        ScanRequest request =
            new ScanRequest()
                .withTableName(v2Request.getTableName())
                .withAttributesToGet(v2Request.getAttributesToGet())
                .withLimit(v2Request.getLimit())
                .withScanFilter(MapV2ScanFilterToV1(v2Request.getScanFilter()))
                .withExclusiveStartKey(MapV2KeyToV1(v2Request.getExclusiveStartKey(), table))
                ;
        request.setCount("COUNT".equals(v2Request.getSelect()));

        return request;
    }

    public static QueryRequest MapV2QueryRequestToV1(
            com.amazonaws.services.dynamodbv2.model.QueryRequest v2Request,
            Table table) {

        QueryRequest request =
            new QueryRequest()
                .withTableName(v2Request.getTableName())
                .withAttributesToGet(v2Request.getAttributesToGet())
                .withLimit(v2Request.getLimit())
                .withConsistentRead(v2Request.getConsistentRead())
                .withScanIndexForward(v2Request.getScanIndexForward())
                .withExclusiveStartKey(MapV2KeyToV1(v2Request.getExclusiveStartKey(), table))
                ;

        request.setCount("COUNT".equals(v2Request.getSelect()));

        if (v2Request.getKeyConditions() != null) {
            for (String v2AttrName : v2Request.getKeyConditions().keySet()) {
                com.amazonaws.services.dynamodbv2.model.Condition v2Condition = v2Request.getKeyConditions().get(v2AttrName);
                if (v2AttrName.equals(table.getHashKeyName())) {
                    List<com.amazonaws.services.dynamodbv2.model.AttributeValue> v2AttrValues = v2Condition.getAttributeValueList();
                    if (v2AttrName.length() > 0) {
                        request.setHashKeyValue(MapV2AttributeValueToV1(v2AttrValues.get(0)));
                    }
                } else if (v2AttrName.equals(table.getRangeKeyName())) {
                    request.setRangeKeyCondition(MapV2ConditionToV1(v2Condition));
                }
            }
        }

        return request;
    }

    public static com.amazonaws.services.dynamodbv2.model.CreateTableResult MapV1CreateTableResultToV2(
            CreateTableResult result) {

        TableDescription table = result.getTableDescription();
        com.amazonaws.services.dynamodbv2.model.TableDescription v2Table = MapV2TableDescriptionToV1(table);
        com.amazonaws.services.dynamodbv2.model.CreateTableResult v2result =
            new com.amazonaws.services.dynamodbv2.model.CreateTableResult()
                .withTableDescription(v2Table)
                ;
        return v2result;
    }

    public static com.amazonaws.services.dynamodbv2.model.DescribeTableResult MapV1DescribeTableResultToV2(
            DescribeTableResult result) {

        TableDescription table = result.getTable();
        com.amazonaws.services.dynamodbv2.model.TableDescription v2Table = MapV2TableDescriptionToV1(table);
        com.amazonaws.services.dynamodbv2.model.DescribeTableResult v2result =
            new com.amazonaws.services.dynamodbv2.model.DescribeTableResult()
                .withTable(v2Table)
                ;
        return v2result;
    }

    public static com.amazonaws.services.dynamodbv2.model.ListTablesResult MapV1ListTablesResultToV2(
            ListTablesResult result) {

        com.amazonaws.services.dynamodbv2.model.ListTablesResult v2result =
            new com.amazonaws.services.dynamodbv2.model.ListTablesResult()
                .withTableNames(result.getTableNames())
                .withLastEvaluatedTableName(result.getLastEvaluatedTableName())
                ;
        return v2result;
    }

    public static com.amazonaws.services.dynamodbv2.model.DeleteTableResult MapV1DeleteTableResultToV2(
            DeleteTableResult result) {

        TableDescription table = result.getTableDescription();
        com.amazonaws.services.dynamodbv2.model.TableDescription v2Table = MapV2TableDescriptionToV1(table);
        com.amazonaws.services.dynamodbv2.model.DeleteTableResult v2result =
            new com.amazonaws.services.dynamodbv2.model.DeleteTableResult()
                .withTableDescription(v2Table)
                ;
        return v2result;
    }

    public static com.amazonaws.services.dynamodbv2.model.UpdateTableResult MapV1UpdateTableResultToV2(
            UpdateTableResult result) {

        TableDescription table = result.getTableDescription();
        com.amazonaws.services.dynamodbv2.model.TableDescription v2Table = MapV2TableDescriptionToV1(table);
        com.amazonaws.services.dynamodbv2.model.UpdateTableResult v2result =
            new com.amazonaws.services.dynamodbv2.model.UpdateTableResult()
                .withTableDescription(v2Table)
                ;
        return v2result;
    }

    public static com.amazonaws.services.dynamodbv2.model.PutItemResult MapV1PutItemResultToV2(
            PutItemResult result,
            String tableName) {

        com.amazonaws.services.dynamodbv2.model.PutItemResult v2result =
            new com.amazonaws.services.dynamodbv2.model.PutItemResult()
                .withAttributes(MapV1ItemToV2(result.getAttributes()))
                .withConsumedCapacity(
                    new com.amazonaws.services.dynamodbv2.model.ConsumedCapacity()
                        .withTableName(tableName)
                        .withCapacityUnits(result.getConsumedCapacityUnits())
                );
        return v2result;
    }

    public static com.amazonaws.services.dynamodbv2.model.GetItemResult MapV1GetItemResultToV2(
            GetItemResult result,
            String tableName) {

        com.amazonaws.services.dynamodbv2.model.GetItemResult v2result =
            new com.amazonaws.services.dynamodbv2.model.GetItemResult()
                .withItem(MapV1ItemToV2(result.getItem()))
                .withConsumedCapacity(
                    new com.amazonaws.services.dynamodbv2.model.ConsumedCapacity()
                        .withTableName(tableName)
                        .withCapacityUnits(result.getConsumedCapacityUnits())
                );
        return v2result;
    }

    public static com.amazonaws.services.dynamodbv2.model.DeleteItemResult MapV1DeleteItemResultToV2(
            DeleteItemResult result,
            String tableName) {

        com.amazonaws.services.dynamodbv2.model.DeleteItemResult v2result =
            new com.amazonaws.services.dynamodbv2.model.DeleteItemResult()
                .withAttributes(MapV1ItemToV2(result.getAttributes()))
                .withConsumedCapacity(
                    new com.amazonaws.services.dynamodbv2.model.ConsumedCapacity()
                        .withTableName(tableName)
                        .withCapacityUnits(result.getConsumedCapacityUnits())
                );
        return v2result;
    }

    public static com.amazonaws.services.dynamodbv2.model.UpdateItemResult MapV1UpdateItemResultToV2(
            UpdateItemResult result,
            String tableName) {

        com.amazonaws.services.dynamodbv2.model.UpdateItemResult v2result =
            new com.amazonaws.services.dynamodbv2.model.UpdateItemResult()
                .withAttributes(MapV1ItemToV2(result.getAttributes()))
                .withConsumedCapacity(
                    new com.amazonaws.services.dynamodbv2.model.ConsumedCapacity()
                        .withTableName(tableName)
                        .withCapacityUnits(result.getConsumedCapacityUnits())
                );
        return v2result;
    }

    public static com.amazonaws.services.dynamodbv2.model.BatchGetItemResult MapV1BatchGetItemResultToV2(
            BatchGetItemResult result,
            Map<String, Table> tables) {

        Map<String, List<Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue>>> v2Responses =
                new HashMap<String, List<Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue>>>();
        List<com.amazonaws.services.dynamodbv2.model.ConsumedCapacity> v2Capacities =
                new ArrayList<com.amazonaws.services.dynamodbv2.model.ConsumedCapacity>();
        for (String tableName : result.getResponses().keySet()) {
            BatchResponse v1Response = result.getResponses().get(tableName);

            List<Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue>> v2Items =
                    new ArrayList<Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue>>();
            for (Map<String, AttributeValue> v1Item : v1Response.getItems()) {
                v2Items.add(MapV1ItemToV2(v1Item));
            }

            v2Capacities.add(
                new com.amazonaws.services.dynamodbv2.model.ConsumedCapacity()
                    .withTableName(tableName)
                    .withCapacityUnits(v1Response.getConsumedCapacityUnits())
                    );
        }

        Map<String, com.amazonaws.services.dynamodbv2.model.KeysAndAttributes> v2UnprocessedKeys = null;
        Map<String, KeysAndAttributes> v1UnprocessedKeys = result.getUnprocessedKeys();
        if (v1UnprocessedKeys != null) {
            v2UnprocessedKeys = new HashMap<String, com.amazonaws.services.dynamodbv2.model.KeysAndAttributes>();
            for (String tableName : v1UnprocessedKeys.keySet()) {
                Table table = tables.get(tableName);
                KeysAndAttributes v1KeysAndAttr = result.getUnprocessedKeys().get(tableName);
                v2UnprocessedKeys.put(tableName, MapV1KeysAndAttributesToV2(
                        v1KeysAndAttr, table));
            }
        }

        com.amazonaws.services.dynamodbv2.model.BatchGetItemResult v2result =
            new com.amazonaws.services.dynamodbv2.model.BatchGetItemResult()
                .withResponses(v2Responses)
                .withUnprocessedKeys(v2UnprocessedKeys)
                .withConsumedCapacity(v2Capacities)
                ;
        return v2result;
    }

    public static com.amazonaws.services.dynamodbv2.model.ScanResult MapV1ScanResultToV2(
            ScanResult result,
            Table table) {

        List<Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue>> v2Items = null;
        if (result.getItems() != null) {
            v2Items = new ArrayList<Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue>>();
            for (Map<String, AttributeValue> v1Item : result.getItems()) {
                v2Items.add(MapV1ItemToV2(v1Item));
            }
        }
        com.amazonaws.services.dynamodbv2.model.ScanResult v2result =
            new com.amazonaws.services.dynamodbv2.model.ScanResult()
                .withItems(v2Items)
                .withCount(result.getCount())
                .withScannedCount(result.getScannedCount())
                .withLastEvaluatedKey(MapV1KeyToV2(result.getLastEvaluatedKey(), table))
                .withConsumedCapacity(
                    new com.amazonaws.services.dynamodbv2.model.ConsumedCapacity()
                        .withTableName(table.getName())
                        .withCapacityUnits(result.getConsumedCapacityUnits())
                );
        return v2result;
    }

    public static com.amazonaws.services.dynamodbv2.model.QueryResult MapV1QueryResultToV2(
            QueryResult result,
            Table table) {

        List<Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue>> v2Items = null;
        if (result.getItems() != null) {
            v2Items = new ArrayList<Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue>>();
            for (Map<String, AttributeValue> v1Item : result.getItems()) {
                v2Items.add(MapV1ItemToV2(v1Item));
            }
        }
        com.amazonaws.services.dynamodbv2.model.QueryResult v2result =
            new com.amazonaws.services.dynamodbv2.model.QueryResult()
                .withItems(v2Items)
                .withCount(result.getCount())
                .withLastEvaluatedKey(MapV1KeyToV2(result.getLastEvaluatedKey(), table))
                .withConsumedCapacity(
                    new com.amazonaws.services.dynamodbv2.model.ConsumedCapacity()
                        .withTableName(table.getName())
                        .withCapacityUnits(result.getConsumedCapacityUnits())
                );
        return v2result;
    }
}

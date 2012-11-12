/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "mapper.TestClassWithRangeHashKey")
public class TestClassWithHashRangeKey
{

    private String hashCode;
    private String rangeCode;
    private String stringData;
    private int intData;

    @DynamoDBHashKey(attributeName = "hashCode")
    public final String getHashCode()
    {
        return hashCode;
    }

    public final void setHashCode(String hashCode)
    {
        this.hashCode = hashCode;
    }

    @DynamoDBRangeKey(attributeName = "rangeCode")
    public final String getRangeCode()
    {
        return rangeCode;
    }

    public final void setRangeCode(String rangeCode)
    {
        this.rangeCode = rangeCode;
    }

    @DynamoDBAttribute(attributeName = "stringData")
    public String getStringData()
    {
        return stringData;
    }

    public void setStringData(String stringData)
    {
        this.stringData = stringData;
    }

    @DynamoDBAttribute(attributeName = "intData")
    public int getIntData()
    {
        return intData;
    }

    public void setIntData(int intData)
    {
        this.intData = intData;
    }
}

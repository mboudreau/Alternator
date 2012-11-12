/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michelboudreau.test;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "mapper.TestClassWithHashKey")
public class TestClassWithHashKey
{

    private String code;
    private String stringData;
    private int intData;

    @DynamoDBHashKey(attributeName = "code")
    public final String getCode()
    {
        return code;
    }

    public final void setCode(String code)
    {
        this.code = code;
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

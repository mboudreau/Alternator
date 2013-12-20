/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michelboudreau.testv2;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.util.Set;

@DynamoDBTable(tableName = "mapper.TestClassWithHashKey")
public class TestClassWithHashKey
{

    private String code;
    private String stringData;
    private int intData;
    private Set<String> stringSetData;
    private Set<Integer> numberSetData;

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

    @DynamoDBAttribute(attributeName = "stringSetData")
    public Set<String> getStringSetData() {
        return stringSetData;
    }

    public void setStringSetData(Set<String> stringSetData) {
        this.stringSetData = stringSetData;
    }

    @DynamoDBAttribute(attributeName = "numberSetData")
    public Set<Integer> getNumberSetData() {
        return numberSetData;
    }

    public void setNumberSetData(Set<Integer> numberSetData) {
        this.numberSetData = numberSetData;
    }
}

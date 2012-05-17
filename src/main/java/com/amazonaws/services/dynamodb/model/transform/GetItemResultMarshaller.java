package com.amazonaws.services.dynamodb.model.transform;


import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodb.model.*;
import com.amazonaws.transform.Marshaller;
import com.amazonaws.util.json.JSONWriter;

import java.io.StringWriter;

public class GetItemResultMarshaller implements Marshaller<String, GetItemResult> {

	public String marshall(GetItemResult getItemResult) {
		if (getItemResult == null) {
			throw new AmazonClientException("Invalid argument passed to marshall(...)");
		}

        try {
            StringWriter stringWriter = new StringWriter();
            JSONWriter jsonWriter = new JSONWriter(stringWriter);
            jsonWriter.object();

            if (getItemResult.getItem() != null) {
                jsonWriter.key("Item").object();
                for(String key : getItemResult.getItem().keySet()) {
                    AttributeValue value = getItemResult.getItem().get(key);
                    jsonWriter.key(key).object();
                    if(value.getN()!=null){
                        jsonWriter.key("N").value(value.getN());
                    }else if(value.getS()!=null){
                        jsonWriter.key("S").value(value.getS());
                    } else if(value.getSS()!=null){
                        jsonWriter.key("SS").value(value.getSS());
                    }else if(value.getNS()!=null){
                        jsonWriter.key("NS").value(value.getNS());
                    }
                    jsonWriter.endObject();
                }
                jsonWriter.endObject();
            }
            jsonWriter.key("ConsumedCapacityUnits").value(0.5);
            jsonWriter.endObject();
            return stringWriter.toString();
        } catch (Throwable t) {
            throw new AmazonClientException("Unable to marshall request to JSON: " + t.getMessage(), t);
        }
	}
}

package com.michelboudreau.alternator.parsers;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.Unmarshaller;
import com.michelboudreau.alternator.enums.RequestType;
import java.io.BufferedReader;
import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmazonWebServiceRequestParser {
	private final Logger logger = LoggerFactory.getLogger(AmazonWebServiceRequestParser.class);
	private final JsonFactory jsonFactory = new JsonFactory();
	private HttpServletRequest request;
    private String apiVersion;
	private RequestType type;
	private AmazonWebServiceRequest data;

	public AmazonWebServiceRequestParser(HttpServletRequest request) {
		this.request = request;
	}

	public String getApiVersion() {
		if (this.apiVersion == null) {
			String str = null;
			String header = request.getHeader("x-amz-target");
			if (header != null) {
				String[] array = header.split("[.]"); //  header comes back as DynamoDB_<apiversion as YYYYMMDD>.<request string>
				if (array.length > 0) {
					str = array[0];
				}
			}
			this.apiVersion = str;
		}
		return this.apiVersion;
	}

	public RequestType getType() {
		if (this.type == null) {
			String str = null;
			String header = request.getHeader("x-amz-target");
			if (header != null) {
				String[] array = header.split("[.]"); //  header comes back as DynamoDB_20111205.<request string>
				if (array.length > 1) {
					str = array[1];
				}
			}
			this.type = RequestType.fromString(str);
		}
		return this.type;
	}

	public <T extends AmazonWebServiceRequest> T getData(Class<T> clazz, Unmarshaller<T, JsonUnmarshallerContext> unmarshaller) {
		ObjectMapper mapper = new ObjectMapper();
		String json = getPostString();
        if (json != null) {
			try {
				JsonParser jsonParser = jsonFactory.createJsonParser(json);
				try {
					JsonUnmarshallerContext unmarshallerContext = new JsonUnmarshallerContext(jsonParser);
					T result = unmarshaller.unmarshall(unmarshallerContext);
					return result;
				} finally {
				}
				//return mapper.readValue(json, clazz);
			} catch (Exception e) {
				logger.error("Could not read JSON into class: " + e);
			}
		} else {
			logger.warn("Not POST data could be retrieved");
		}
		return null;
	}

	protected String getPostString() {
		String json = null;
		if (request.getMethod() == "POST") {
			try {
				BufferedReader reader = request.getReader();
				StringBuilder sb = new StringBuilder();
				String line = reader.readLine();
				while (line != null) {
					sb.append(line + "\n");
					line = reader.readLine();
				}
				reader.close();
				json = sb.toString();
			} catch (IOException e) {
				logger.warn("Could not retrieve POST data from request");
			}
		}
		return json;
	}

}

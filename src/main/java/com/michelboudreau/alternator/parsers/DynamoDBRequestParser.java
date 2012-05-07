package com.michelboudreau.alternator.parsers;

import com.michelboudreau.alternator.enums.RequestType;
import com.michelboudreau.alternator.models.request.DynamoDBRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;

public class DynamoDBRequestParser {
	private final Logger logger = LoggerFactory.getLogger(DynamoDBRequestParser.class);
	private RequestType type;
	private DynamoDBRequest data;

	public DynamoDBRequestParser(HttpServletRequest request) {
		this.type = getTypeFromRequest(request);
		this.data = getDataFromPost(request);
	}

	public RequestType getType() {
		return type;
	}

	public DynamoDBRequest getData() {
		return data;
	}

	protected RequestType getTypeFromRequest(HttpServletRequest request) {
		String type = null;
		String header = request.getHeader("x-amz-target");
		if (header != null) {
			String[] array = header.split("[.]"); //  header comes back as DynamoDB_20111205.<request string>
			if (array.length > 1) {
				type = array[1];
			}
		}
		return RequestType.fromString(type);
	}

	protected DynamoDBRequest getDataFromPost(HttpServletRequest request) {
		DynamoDBRequest data = null;
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
				String json = sb.toString();

				// Create object with string
				ObjectMapper mapper = new ObjectMapper();
				mapper.readValue(json, DynamoDBRequest.class);
			} catch (IOException e) {
				logger.warn("Could not retrieve POST data from request");
			}
		}
		return data;
	}
}

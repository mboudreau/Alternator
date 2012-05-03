package com.michelboudreau.alternator;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
import com.michelboudreau.alternator.controller.WebController;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
final class AlternatorDBConfig {

	@Bean
	public AlternatorDBClient client() {
		return new AlternatorDBClient();
	}

	@Bean
	public DynamoDBMapper mapper() {
		return new DynamoDBMapper(client());
	}

	@Bean
	public WebController webController() {
		return new WebController();
	}

	@Bean
	public AlternatorDBHandler alternatorDBHandler() {
		return new AlternatorDBHandler();
	}
}

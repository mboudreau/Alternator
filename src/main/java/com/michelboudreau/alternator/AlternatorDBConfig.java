package com.michelboudreau.alternator;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class AlternatorDBConfig {

	@Bean
	public AlternatorDBClient client() {
		return new AlternatorDBClient();
	}

	@Bean
	public DynamoDBMapper mapper() {
		return new DynamoDBMapper(client());
	}

	@Bean
	public AlternatorDBController webController() {
		return new AlternatorDBController();
	}
}

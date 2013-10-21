package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.List;

public class DescribeTableRequestValidator extends Validator {

	public Boolean supports(Class clazz) {
		return DescribeTableRequest.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		DescribeTableRequest instance = (DescribeTableRequest) target;
		List<Error> errors = ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName());
		return removeNulls(errors);
	}
}

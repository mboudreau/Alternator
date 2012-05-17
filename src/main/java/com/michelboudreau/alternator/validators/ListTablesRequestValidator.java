package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.ListTablesRequest;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.ArrayList;
import java.util.List;

public class ListTablesRequestValidator extends Validator {

	public Boolean supports(Class clazz) {
		return ListTablesRequest.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		ListTablesRequest instance = (ListTablesRequest) target;
		List<Error> errors = new ArrayList<Error>();
		if(instance.getExclusiveStartTableName() != null) {
			errors.addAll(ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getExclusiveStartTableName()));
		}

		if(instance.getLimit() != null) {
			errors.addAll(ValidatorUtils.rejectIfSizeOutOfBounds(instance.getLimit(), 1, 100));
		}
		return removeNulls(errors);
	}
}

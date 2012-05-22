package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.michelboudreau.alternator.models.Limits;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.List;

public class QueryRequestValidator extends Validator {

	public Boolean supports(Class clazz) {
		return QueryRequest.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		QueryRequest instance = (QueryRequest) target;
		List<Error> errors = ValidatorUtils.invokeValidator(new TableNameValidator(), instance.getTableName());
		errors.addAll(ValidatorUtils.invokeValidator(new PrimaryKeyValidator(), instance.getHashKeyValue()));
		if(errors.size() == 0) {
			if(instance.getLimit() != null) {
				errors.addAll(ValidatorUtils.rejectIfSizeOutOfBounds(instance.getLimit(), 1, Limits.NUMBER_MAX));
			}
			if(instance.getAttributesToGet() != null) {
				for(String attr:instance.getAttributesToGet()) {
					errors.addAll(ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(attr));
				}
			}
			if(instance.getRangeKeyCondition() != null) {
				errors.addAll(ValidatorUtils.invokeValidator(new ConditionValidator(), instance.getRangeKeyCondition()));
			}
			if(instance.getExclusiveStartKey() != null) {
				errors.addAll(ValidatorUtils.invokeValidator(new PrimaryKeyValidator(), instance.getExclusiveStartKey().getHashKeyElement()));
				if(instance.getExclusiveStartKey().getRangeKeyElement() != null) {
					errors.addAll(ValidatorUtils.invokeValidator(new PrimaryKeyValidator(), instance.getExclusiveStartKey().getRangeKeyElement()));
				}
			}
		}
		return removeNulls(errors);
	}
}

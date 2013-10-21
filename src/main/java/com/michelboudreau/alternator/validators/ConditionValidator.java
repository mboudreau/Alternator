package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Condition;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.util.List;

public class ConditionValidator extends Validator {

	public Boolean supports(Class clazz) {
		return Condition.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		Condition instance = (Condition) target;
		List<Error> errors = ValidatorUtils.rejectIfNull(instance);
		if (errors.size() == 0) {
			errors.addAll(ValidatorUtils.rejectIfNullOrEmptyOrWhitespace(instance.getComparisonOperator()));
			errors.addAll(ValidatorUtils.rejectIfNotMatchRegex(instance.getComparisonOperator(), "^(EQ|NE|IN|LE|LT|GE|GT|BETWEEN|NOT_NULL|NULL|CONTAINS|NOT_CONTAINS|BEGINS_WITH)$"));
			if(instance.getAttributeValueList() != null) {
				for(AttributeValue value:instance.getAttributeValueList()) {
					errors.addAll(ValidatorUtils.invokeValidator(new PrimaryKeyValidator(), value));
				}
			}
		}
		return removeNulls(errors);
	}
}

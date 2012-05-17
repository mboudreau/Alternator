package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.ExpectedAttributeValue;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExpectedValidator extends Validator {

	public Boolean supports(Class clazz) {
		return String.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		Map<String, ExpectedAttributeValue> instance = (Map<String, ExpectedAttributeValue>) target;
		List<Error> errors = new ArrayList<Error>();
		Charset utf = Charset.forName("UTF-8");
		for (Map.Entry<String, ExpectedAttributeValue> entry : instance.entrySet()) {
			String key = entry.getKey();
			ExpectedAttributeValue value = entry.getValue();
			// Check key size
			errors.addAll(ValidatorUtils.rejectIfSizeOutOfBounds(key, 1, 255));
			// check if value is null
			errors.addAll(ValidatorUtils.rejectIfNull(value));
			if (value.getExists() != null && value.getExists() && value == null) {
				errors.add(new Error("If exists is True, Value mush be set"));
			}
			if (value != null && value.getValue() == null && value.getExists() == null) {
				errors.add(new Error("AttributeValue or Exists must be non-null for Expected to work."));
			}
		}

		return removeNulls(errors);
	}
}

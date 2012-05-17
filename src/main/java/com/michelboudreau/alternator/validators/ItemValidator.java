package com.michelboudreau.alternator.validators;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.michelboudreau.alternator.validation.Validator;
import com.michelboudreau.alternator.validation.ValidatorUtils;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class ItemValidator extends Validator {

	public Boolean supports(Class clazz) {
		return Map.class.isAssignableFrom(clazz);
	}

	public List<Error> validate(Object target) {
		Map<String, AttributeValue> instance = (Map<String, AttributeValue>) target;
		List<Error> errors = ValidatorUtils.rejectIfNull(instance);
		if (errors.size() == 0) {
			Charset utf = Charset.forName("UTF-8");
			for (Map.Entry<String, AttributeValue> entry : instance.entrySet()) {
				String key = entry.getKey();
				AttributeValue value = entry.getValue();
				// Check key size
				errors.addAll(ValidatorUtils.rejectIfSizeOutOfBounds(key, 1, 255));
				// Check value size in DB
				byte[] bytes = (key + value.toString()).getBytes(utf);
				errors.addAll(ValidatorUtils.rejectIfSizeOutOfBounds(bytes, 0, 65536));
				// check if value is null
				errors.addAll(ValidatorUtils.rejectIfNull(value));
			}
		}

		return removeNulls(errors);
	}
}

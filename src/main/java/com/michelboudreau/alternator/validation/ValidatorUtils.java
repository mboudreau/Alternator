package com.michelboudreau.alternator.validation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

public class ValidatorUtils {
	private static Log logger = LogFactory.getLog(ValidatorUtils.class);

	public static Error[] invokeValidator(Validator validator, Object obj) {
		Assert.notNull(validator, "Validator must not be null");
		if (logger.isDebugEnabled()) {
			logger.debug("Invoking validator [" + validator + "]");
		}
		if (obj != null && !validator.supports(obj.getClass())) {
			throw new IllegalArgumentException("Validator [" + validator.getClass() + "] does not support [" + obj.getClass() + "]");
		} else {
			return validator.validate(obj);
		}
	}

	public static Error rejectIfNull(String property) {
		/*Object value = errors.getFieldValue(field);
		if (value == null || !StringUtils.hasText(value.toString())) {
			errors.rejectValue(field, errorCode, errorArgs, defaultMessage);
		}*/
		return null;
	}

	public static Error rejectIfNullOrEmpty(String property) {
		/*Assert.
		rejectIfEmpty(property);
		Object value = errors.getFieldValue(field);
		if (value == null || !StringUtils.hasText(value.toString())) {
			errors.rejectValue(field, errorCode, errorArgs, defaultMessage);
		}*/
		return null;
	}

	public static Error rejectIfNullOrEmptyOrWhitespace(String property) {
		/*Assert.
		rejectIfEmpty(property);
		Object value = errors.getFieldValue(field);
		if (value == null || !StringUtils.hasText(value.toString())) {
			errors.rejectValue(field, errorCode, errorArgs, defaultMessage);
		}*/
		return null;
	}
}

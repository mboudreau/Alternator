package com.michelboudreau.alternator.validation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ValidatorUtils {
	private static Log logger = LogFactory.getLog(ValidatorUtils.class);

	public static List<Error> invokeValidator(Validator validator, Object obj) {
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

	public static <T> List<Error> rejectIfNull(T property) {
		List<Error> errors = new ArrayList<Error>();
		if (property == null) {
			errors.add(new Error("property value is null"));
		}
		return errors;
	}

	public static <T> List<Error> rejectIfEmpty(T property) {
		List<Error> errors = new ArrayList<Error>();
		Boolean empty = false;
		if (property instanceof String) {
			String string = (String) property;
			empty = string.length() == 0;
		} else if (property.getClass().isArray()) {
			T[] array = (T[]) property;
			empty = array.length == 0;
		} else if (property instanceof Collection) {
			Collection<?> coll = (Collection) property;
			empty = coll.size() == 0;
		} else {
			errors.add(new Error("The property type is not recognized"));
		}

		if (empty) {
			errors.add(new Error("property value is empty"));
		}
		return errors;
	}

	public static <T> List<Error> rejectIfWhitespace(T property) {
		List<Error> errors = new ArrayList<Error>();
		Boolean empty = false;
		if (property instanceof String) {
			String string = (String) property;
			string = string.trim();
			empty = string.length() == 0;
		} else {
			errors.add(new Error("The property type is not recognized"));
		}

		if (empty) {
			errors.add(new Error("property value is empty"));
		}
		return errors;
	}

	public static <T> List<Error> rejectIfNullOrEmptyOrWhitespace(T property) {
		List<Error> errors = rejectIfNull(property);
		if (errors.size() == 0) {
			errors = rejectIfEmpty(property);
			if (errors.size() == 0) {
				errors = rejectIfWhitespace(property);
			}
		}
		return errors;
	}

	public static <T> List<Error> rejectIfNotMatchRegex(T property, String regex) {
		List<Error> errors = new ArrayList<Error>();
		if (property instanceof String) {
			String string = (String) property;
			Pattern p = Pattern.compile(regex);
			Matcher m = p.matcher(string);
			if (!m.matches()) {
				errors.add(new Error("property is out of bounds of regex: " + regex));
			}
		} else {
			errors.add(new Error("The property type is not recognized"));
		}

		return errors;
	}

	public static <T> List<Error> rejectIfSizeOutOfBounds(T property, int min, int max) {
		List<Error> errors = new ArrayList<Error>();
		Boolean outOfBounds = false;
		if (property instanceof String) {
			String string = (String) property;
			outOfBounds = (string.length() < min || string.length() > max);
		} else if (property.getClass().isArray()) {
			T[] array = (T[]) property;
			outOfBounds = (array.length < min || array.length > max);
		} else if (property instanceof Collection) {
			Collection<?> coll = (Collection) property;
			outOfBounds = (coll.size() < min || coll.size() > max);
		} else {
			errors.add(new Error("The property type is not recognized"));
		}

		if (outOfBounds) {
			errors.add(new Error("property is out of provided bounds."));
		}

		return errors;
	}
}

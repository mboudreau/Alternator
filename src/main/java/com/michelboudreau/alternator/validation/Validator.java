package com.michelboudreau.alternator.validation;

public interface Validator {
	boolean supports(Class<?> clazz);
	Error[] validate(Object target);
}

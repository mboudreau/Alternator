package com.michelboudreau.alternator.validation;

import java.util.List;

public interface Validator {
	boolean supports(Class<?> clazz);
    List<Error> validate(Object target);
}

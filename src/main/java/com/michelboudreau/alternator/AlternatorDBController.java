package com.michelboudreau.alternator;

import com.michelboudreau.alternator.AlternatorDBHandler;
import com.michelboudreau.alternator.models.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;

@Controller
@RequestMapping(value = "/", produces = "application/json")
class AlternatorDBController {

	private AlternatorDBHandler handler = new AlternatorDBHandler();

	public AlternatorDBController() {
	}

	@ResponseStatus(HttpStatus.OK)
	@RequestMapping(method = RequestMethod.POST, consumes = "application/x-amz-json-1.0")
	@ResponseBody
	public Object alternatorDBController(HttpServletRequest request) {
		Object obj = handler.handle(request);
		return obj;
	}

	/*@ResponseStatus(HttpStatus.OK)
	@RequestMapping(value = "/tables", method = RequestMethod.GET)
	@ResponseBody
	public Iterable<Table> getTables() {
		return handler.getTables();
	}*/

	/*@ResponseStatus(HttpStatus.OK)
	@RequestMapping(value = "/models", method = RequestMethod.GET)
	@ResponseBody
	public AlternatorDB getData() {
		return handler;
	}*/
}

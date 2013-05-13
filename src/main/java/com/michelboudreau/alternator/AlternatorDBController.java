package com.michelboudreau.alternator;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodb.model.InternalServerErrorException;
import com.amazonaws.services.dynamodb.model.transform.AmazonServiceExceptionMarshaller;

@Controller
@RequestMapping(value = "/", produces = "application/x-amz-json-1.0")
class AlternatorDBController {

    private ServletContext servletContext;

	private AlternatorDBHandler handler = new AlternatorDBHandler();

	public AlternatorDBController() {
	}

    @PostConstruct
    public void init() {
        String persistenceLocation = servletContext.getInitParameter(AlternatorDB.PERSISTENCE_LOCATION);
        if (persistenceLocation != null) {
            handler.restore(persistenceLocation);
        }
    }

    @PreDestroy
    public void destroy() {
        String persistenceLocation = servletContext.getInitParameter(AlternatorDB.PERSISTENCE_LOCATION);
	    String sandboxStatus = servletContext.getInitParameter(AlternatorDB.SANDBOX_STATUS);
        if (persistenceLocation != null && sandboxStatus.equals("false")) {
            handler.save(persistenceLocation);
        }
    }

	@RequestMapping(method = RequestMethod.POST, consumes = "application/x-amz-json-1.0")
	public ResponseEntity<String> alternatorDBController(HttpServletRequest request, HttpServletResponse response) {
		try {
			try {
				request.setCharacterEncoding("UTF-8");
			} catch (Exception e) {
				e.printStackTrace();
			}
			HttpHeaders responseHeaders = new HttpHeaders();
			responseHeaders.add("Content-Type", "application/json; charset=utf-8");
            String jsonResult = handler.handle(request);
			return new ResponseEntity<String>(jsonResult, responseHeaders, HttpStatus.OK);
		} catch (AmazonServiceException e) {
			int statusCode = 400;
			if (e instanceof InternalServerErrorException) {
				statusCode = 500;
			}
			ResponseEntity responseEntity = new ResponseEntity<String>(new AmazonServiceExceptionMarshaller().marshall(e), new HttpHeaders(), HttpStatus.valueOf(statusCode));
            return responseEntity;
		}
	}

    @Inject
    public void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
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

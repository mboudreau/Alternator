package com.michelboudreau.alternator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import com.sun.grizzly.http.SelectorThread;
import com.sun.grizzly.http.servlet.ServletAdapter;
import com.sun.grizzly.standalone.StaticStreamAlgorithm;

public class Server {

	private final int port;
	private final SelectorThread selectorThread;

	public Server(int port) {
		if (port < 0 || port > 65535)
			throw new IllegalArgumentException("Bad port");
		this.port = port;

		ServletAdapter servletAdapter = new ServletAdapter();
		servletAdapter
				.addContextParameter("contextConfigLocation",
						" classpath:com/michelboudreau/alternator/spring/applicationContext.xml");
		servletAdapter
				.addServletListener("org.springframework.web.context.ContextLoaderListener");
		servletAdapter
				.setServletInstance(new org.springframework.web.servlet.DispatcherServlet());
		servletAdapter
				.addInitParameter("contextConfigLocation",
						"classpath:com/michelboudreau/alternator/spring/applicationServlet.xml");
		Map<String, String> initParams = new HashMap<String, String>();
		initParams.put("encoding", "UTF-8");
		initParams.put("forceEncoding", "true");

		servletAdapter.addFilter(
				new org.springframework.web.filter.CharacterEncodingFilter(),
				"characterEncodingFilter", initParams);
		selectorThread = new SelectorThread();
		servletAdapter.setResourcesContextPath(getURI().getRawPath());
		selectorThread.setAlgorithmClassName(StaticStreamAlgorithm.class
				.getName());
		selectorThread.setPort(port);
		selectorThread.setAdapter(servletAdapter);
	}

	public void start() {
		try {
			selectorThread.listen();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		}
	}

	public void stop() {
		selectorThread.stopEndpoint();
	}

	public URI getURI() {
		try {
			return new URI("http://127.0.0.1:" + port);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void main(String[] args) {
		Server s = new Server(9090);
		s.start();
		
	}
}

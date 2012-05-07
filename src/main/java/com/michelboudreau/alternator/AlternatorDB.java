package com.michelboudreau.alternator;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

public class AlternatorDB {

	private final Logger logger = LoggerFactory.getLogger(AlternatorDB.class);
	private Server server;
	private ServletContextHandler context;

	public AlternatorDB() {
		this(9090);
	}

	public AlternatorDB(int port) {
		if (port == 0) {
			port = 9090;
		}

		// Create server
		this.server = new Server();
		SelectChannelConnector connector = new SelectChannelConnector();
		connector.setHost("localhost");
		connector.setPort(port);
		this.server.addConnector(connector);

		// Create context
		this.context = new ServletContextHandler(this.server, "/", ServletContextHandler.SESSIONS);
		this.context.setContextPath("/");
		this.context.setInitParameter("contextClass", AnnotationConfigWebApplicationContext.class.getName());
		this.context.setInitParameter("contextConfigLocation", AlternatorDBConfig.class.getName());

		// Add listener
		ContextLoaderListener listener = new ContextLoaderListener();
		this.context.addEventListener(listener);

		// Create servlet
		DispatcherServlet servlet = new DispatcherServlet();
		ServletHolder holder = new ServletHolder(servlet);
		holder.setInitOrder(1);
		holder.setInitParameter("contextClass", AnnotationConfigWebApplicationContext.class.getName());
		holder.setInitParameter("contextConfigLocation", AlternatorDBConfig.class.getName());
		this.context.addServlet(holder, "/");
	}

	public AlternatorDB start() throws Exception {
		this.server.start();
		return this;
	}

	public AlternatorDB stop() throws Exception {
		this.server.stop();
		return this;
	}

	public boolean reset() {
		return true;
	}
}

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

import java.io.File;

public class AlternatorDB {

	public static final String PERSISTENCE_LOCATION = "persistence-location";
	public static final String SANDBOX_STATUS = "sandbox-status";

	private final Logger logger = LoggerFactory.getLogger(AlternatorDB.class);
	private Server server;
	private ServletContextHandler context;

	public AlternatorDB() {
		this(9090, null, true);
	}

	public AlternatorDB(int port) {
		this(port, null, true);
	}

	public AlternatorDB(int port, File persistence) {
		this(port, null, true);
	}

	public AlternatorDB(int port, File persistence, Boolean sanboxStatus) {
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
		if (persistence != null) {
			this.context.setInitParameter(PERSISTENCE_LOCATION, persistence.getAbsolutePath());
		}
		this.context.setInitParameter(SANDBOX_STATUS, sanboxStatus + "");

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

	public AlternatorDB join() throws Exception {
		this.server.join();
		return this;
	}

	public AlternatorDB stop() throws Exception {
		this.server.stop();
		return this;
	}

	public AlternatorDB restart() throws Exception {
		stop().start();
		return this;
	}
}

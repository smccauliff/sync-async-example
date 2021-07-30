package com.linkedin.example;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class EmbeddedJetty {
  private Server _server;

  public void start() throws Exception {
    _server = new Server();
    ServerConnector connector = new ServerConnector(_server);
    connector.setPort(8090);
    _server.setConnectors(new Connector[]{connector});
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/example");
    _server.setHandler(context);
    context.addServlet(new ServletHolder(new ProduceServlet()), "/*");
    _server.start();
  }

  public static void main(String[] argv) throws Exception {
    EmbeddedJetty embeddedJetty = new EmbeddedJetty();
    embeddedJetty.start();
    System.out.println("Exited main");
  }

}
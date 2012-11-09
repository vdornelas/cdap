package com.continuuity.common.logging;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class FlumeLogCollector {

  private static final Logger LOG = LoggerFactory
      .getLogger(FlumeLogCollector.class);

  private CConfiguration config;

  public FlumeLogCollector(CConfiguration config) {
    this.config = config;
    this.port = config.getInt(Constants.CFG_LOG_COLLECTION_PORT,
        Constants.DEFAULT_LOG_COLLECTION_PORT);
    this.threads = config.getInt(Constants.CFG_LOG_COLLECTION_THREADS,
        Constants.DEFAULT_LOG_COLLECTION_THREADS);
  }

  private Server server;
  private int threads;
  private int port;


  public void start() {

    LOG.info("Starting up " + this);

    // this is all standard avro ipc. The key is to pass in Flume's avro
    // source protocol as the interface, and the FlumeAdapter as its
    // implementation.
    this.server = new NettyServer(
        new SpecificResponder(AvroSourceProtocol.class,
            new FlumeLogAdapter(config)),
        new InetSocketAddress(this.port),
        // in order to control the number of netty worker threads, we
        // must create and pass in the server channel factory explicitly
        new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
            this.threads));
    this.server.start();

    LOG.info("Log Collection Source started on port " + port + " with "
        + this.threads + " threads.");
  }

  public static void main(String[] args) {
    CConfiguration configuration = CConfiguration.create();
    new FlumeLogCollector(configuration).start();
  }

}

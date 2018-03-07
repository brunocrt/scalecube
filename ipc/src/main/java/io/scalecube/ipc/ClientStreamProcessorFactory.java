package io.scalecube.ipc;

import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import rx.subscriptions.CompositeSubscription;

public final class ClientStreamProcessorFactory {

  private static final Bootstrap DEFAULT_BOOTSTRAP;
  // Pre-configure default bootstrap
  static {
    DEFAULT_BOOTSTRAP = new Bootstrap()
        .group(new NioEventLoopGroup(0))
        .channel(NioSocketChannel.class)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true);
  }

  private final ServerStream serverStream;
  private final ClientStream clientStream;
  private final CompositeSubscription subscriptions = new CompositeSubscription();

  private ClientStreamProcessorFactory(ServerStream serverStream, ClientStream clientStream) {
    this.serverStream = serverStream;
    this.clientStream = clientStream;

    // request logic
    subscriptions.add(serverStream.listenWrite()
        .subscribe(event -> clientStream.send(event.getAddress(), event.getMessageOrThrow())));

    // response logic
    subscriptions.add(clientStream.listenMessageReadSuccess()
        .subscribe(message -> serverStream.send(message, ChannelContext::postReadSuccess)));

    // response logic
    subscriptions.add(clientStream.listenWriteError()
        .subscribe(event -> serverStream.send(event.getMessageOrThrow(), (channelContext, message1) -> {
          Address address = event.getAddress();
          Throwable throwable = event.getErrorOrThrow();
          channelContext.postWriteError(address, message1, throwable);
        })));
  }

  /**
   * Factory method. Creates new {@link ClientStreamProcessorFactory} with default bootstrap.
   */
  public static ClientStreamProcessorFactory newClientStreamProcessorFactory() {
    return newClientStreamProcessorFactory(DEFAULT_BOOTSTRAP);
  }

  /**
   * Factory method. Creates new {@link ClientStreamProcessorFactory} with custom {@link Bootstrap}.
   */
  public static ClientStreamProcessorFactory newClientStreamProcessorFactory(Bootstrap bootstrap) {
    ServerStream serverStream = ServerStream.newServerStream();
    ClientStream clientStream = ClientStream.newClientStream(bootstrap);
    return new ClientStreamProcessorFactory(serverStream, clientStream);
  }

  /**
   * @param address target address for processor.
   * @return new {@link ClientStreamProcessor} object with reference to this serverStream and given address.
   */
  public ClientStreamProcessor newClientStreamProcessor(Address address) {
    ChannelContext localContext = ChannelContext.create(address);
    ChannelContext remoteContext = ChannelContext.create(address);
    serverStream.subscribe(remoteContext);
    return new ClientStreamProcessor(localContext, remoteContext);
  }

  /**
   * Closes shared (across {@link ClientStreamProcessor} instances) serverStream and clientStream.
   */
  public void close() {
    serverStream.close();
    clientStream.close();
    subscriptions.clear();
  }
}

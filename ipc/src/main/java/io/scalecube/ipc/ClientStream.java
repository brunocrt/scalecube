package io.scalecube.ipc;

import io.scalecube.ipc.netty.NettyClientTransport;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.concurrent.CompletableFuture;

public final class ClientStream extends DefaultEventStream {

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

  // listens to address not connected
  private final Subject<Address, Address> connectFailedSubject = PublishSubject.<Address>create().toSerialized();

  private NettyClientTransport clientTransport; // calculated

  private ClientStream(Bootstrap bootstrap) {
    clientTransport = new NettyClientTransport(bootstrap, this::subscribe);
    // register cleanup process upfront
    listenClose(aVoid -> {
      clientTransport.close();
      connectFailedSubject.onCompleted();
    });
  }

  public static ClientStream newClientStream() {
    return new ClientStream(DEFAULT_BOOTSTRAP);
  }

  public static ClientStream newClientStream(Bootstrap bootstrap) {
    return new ClientStream(bootstrap);
  }

  /**
   * Sends a message to a given address.
   * 
   * @param address of target endpoint.
   * @param message to send.
   */
  public void send(Address address, ServiceMessage message) {
    CompletableFuture<ChannelContext> promise = clientTransport.getOrConnect(address);
    promise.whenComplete((channelContext, throwable) -> {
      if (channelContext != null) {
        channelContext.postMessageWrite(message);
      }
      if (throwable != null) {
        connectFailedSubject.onNext(address);
      }
    });
  }

  /**
   * Subscription point for listening on failed connection attempts. When connection failed on {@link Address} no
   * {@link ChannelContext} would be created, there by concerned party can't use {@link #listen()} to catch up on failed
   * connection attempts => this method exists.
   */
  public Observable<Address> listenConnectFailed() {
    return connectFailedSubject.onBackpressureBuffer().asObservable();
  }
}

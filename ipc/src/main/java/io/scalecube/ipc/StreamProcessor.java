package io.scalecube.ipc;

import io.scalecube.transport.Address;

import rx.Emitter;
import rx.Observable;

public final class StreamProcessor {

  private static final ServiceMessage onErrorMessage =
      ServiceMessage.withQualifier(Qualifier.Q_GENERAL_FAILURE).build();

  private static final ServiceMessage onCompletedMessage =
      ServiceMessage.withQualifier(Qualifier.Q_ON_COMPLETED).build();

  private final ChannelContext channelContext;
  private final Observable<ServiceMessage> responseStream;

  public StreamProcessor(Address address, ServerStream serverStream) {
    // create 'subscriber'
    serverStream.subscribe(channelContext = ChannelContext.create(address));

    // listen for upstream 'unsubscribe'
    serverStream.listenChannelContextUnsubscribed().subscribe(event -> {
      // Todo ...
    });

    // prepare response stream and account for downstream 'unsubscribe'
    responseStream = Observable.<ServiceMessage>create(
        emitter -> channelContext.listenMessageReadSuccess().flatMap(this::toResponse).subscribe(emitter),
        Emitter.BackpressureMode.BUFFER)
        .doOnUnsubscribe(() -> {
          // Todo ...
        })
        .share();
  }

  public void onNext(ServiceMessage message) {
    channelContext.postMessageWrite(message);
  }

  public void onError(Throwable throwable) {
    channelContext.postMessageWrite(onErrorMessage);
  }

  public void onCompleted() {
    channelContext.postMessageWrite(onCompletedMessage);
  }

  public Observable<ServiceMessage> listen() {
    return responseStream;
  }

  private Observable<? extends ServiceMessage> toResponse(ServiceMessage message) {
    if (Qualifier.Q_GENERAL_FAILURE.equalsIgnoreCase(message.getQualifier())) { // remote => onError
      return Observable.error(new RuntimeException(String.valueOf(500)));
    }
    if (Qualifier.Q_ON_COMPLETED.equalsIgnoreCase(message.getQualifier())) { // remote => onCompleted
      return Observable.empty();
    }
    return Observable.just(message); // remote => normal response
  }
}

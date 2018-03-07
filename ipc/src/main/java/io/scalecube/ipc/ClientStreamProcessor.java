package io.scalecube.ipc;

import rx.Emitter;
import rx.Observable;
import rx.subscriptions.CompositeSubscription;

public final class ClientStreamProcessor implements StreamProcessor {

  private static final ServiceMessage onErrorMessage =
      ServiceMessage.withQualifier(Qualifier.Q_GENERAL_FAILURE).build();

  private static final ServiceMessage onCompletedMessage =
      ServiceMessage.withQualifier(Qualifier.Q_ON_COMPLETED).build();

  private final ChannelContext requestContext;
  private final ChannelContext responseContext;

  public ClientStreamProcessor(ChannelContext requestContext, ChannelContext responseContext) {
    this.requestContext = requestContext;
    this.responseContext = responseContext;
  }

  @Override
  public void onNext(ServiceMessage message) {
    requestContext.postWrite(message);
  }

  @Override
  public void onError(Throwable throwable) {
    onNext(onErrorMessage);
    requestContext.close();
  }

  @Override
  public void onCompleted() {
    onNext(onCompletedMessage);
    requestContext.close();
  }

  @Override
  public Observable<ServiceMessage> listen() {
    return Observable.create(emitter -> {

      CompositeSubscription subscriptions = new CompositeSubscription();
      emitter.setCancellation(subscriptions::clear);

      subscriptions.add(responseContext.listenMessageReadSuccess()
          .flatMap(this::toResponse)
          .subscribe(emitter));

      subscriptions.add(responseContext.listenWriteError()
          .map(Event::getErrorOrThrow)
          .subscribe(emitter::onError));

    }, Emitter.BackpressureMode.BUFFER);
  }

  @Override
  public void close() {
    requestContext.close();
    responseContext.close();
  }

  private Observable<? extends ServiceMessage> toResponse(ServiceMessage message) {
    String qualifier = message.getQualifier();
    if (Qualifier.Q_ON_COMPLETED.asString().equalsIgnoreCase(qualifier)) { // remote => onCompleted
      return Observable.empty();
    }
    String qualifierNamespace = Qualifier.getQualifierNamespace(qualifier);
    if (Qualifier.Q_ERROR_NAMESPACE.equalsIgnoreCase(qualifierNamespace)) { // remote => onError
      // Hint: at this point more sophisticated exception mapping logic is needed
      return Observable.error(new RuntimeException(qualifier));
    }
    return Observable.just(message); // remote => normal response
  }
}

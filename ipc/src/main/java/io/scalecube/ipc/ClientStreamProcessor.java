package io.scalecube.ipc;

import rx.Emitter;
import rx.Observable;
import rx.Subscription;
import rx.subscriptions.CompositeSubscription;

public final class ClientStreamProcessor implements StreamProcessor {

  private static final ServiceMessage onErrorMessage =
      ServiceMessage.withQualifier(Qualifier.Q_GENERAL_FAILURE).build();

  private static final ServiceMessage onCompletedMessage =
      ServiceMessage.withQualifier(Qualifier.Q_ON_COMPLETED).build();

  private final ChannelContext localContext;
  private final ChannelContext remoteContext;
  private final Subscription subscription;

  public ClientStreamProcessor(ChannelContext localContext, ChannelContext remoteContext) {
    this.localContext = localContext;
    this.remoteContext = remoteContext;
    this.subscription = localContext.listenMessageWrite().subscribe(remoteContext::postWrite);
  }

  @Override
  public void onNext(ServiceMessage message) {
    localContext.postWrite(message);
  }

  @Override
  public void onError(Throwable throwable) {
    onNext(onErrorMessage);
    localContext.close();
  }

  @Override
  public void onCompleted() {
    onNext(onCompletedMessage);
    localContext.close();
  }

  @Override
  public Observable<ServiceMessage> listen() {
    return Observable.create(emitter -> {

      CompositeSubscription subscriptions = new CompositeSubscription();
      emitter.setCancellation(subscriptions::clear);

      subscriptions.add(remoteContext.listenMessageReadSuccess()
          .flatMap(this::toResponse)
          .subscribe(emitter));

      subscriptions.add(remoteContext.listenWriteError()
          .map(Event::getErrorOrThrow)
          .subscribe(emitter::onError));

    }, Emitter.BackpressureMode.BUFFER);
  }

  @Override
  public void close() {
    localContext.close();
    remoteContext.close();
    subscription.unsubscribe();
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

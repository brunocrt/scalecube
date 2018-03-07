package io.scalecube.ipc;

import rx.Observable;

public interface StreamProcessor {

  void onNext(ServiceMessage message);

  void onError(Throwable throwable);

  void onCompleted();

  Observable<ServiceMessage> listen();

  void close();
}

package io.scalecube.ipc;

import rx.Observable;

import java.util.function.Consumer;

public interface EventStream {

  void subscribe(ChannelContext channelContext);

  Observable<Event> listen();

  void close();

  void listenClose(Consumer<Void> onClose);

  default Observable<Event> listenReadSuccess() {
    return listen().filter(Event::isReadSuccess);
  }

  default Observable<Event> listenReadError() {
    return listen().filter(Event::isReadError);
  }

  default Observable<Event> listenWrite() {
    return listen().filter(Event::isWrite);
  }

  default Observable<Event> listenWriteSuccess() {
    return listen().filter(Event::isWriteSuccess);
  }

  default Observable<Event> listenWriteError() {
    return listen().filter(Event::isWriteError);
  }

  default Observable<ServiceMessage> listenMessageReadSuccess() {
    return listenReadSuccess().map(Event::getMessageOrThrow);
  }

  default Observable<ServiceMessage> listenMessageWriteSuccess() {
    return listenWriteSuccess().map(Event::getMessageOrThrow);
  }

  default Observable<Event> listenChannelContextClosed() {
    return listen().filter(Event::isChannelContextClosed);
  }
}

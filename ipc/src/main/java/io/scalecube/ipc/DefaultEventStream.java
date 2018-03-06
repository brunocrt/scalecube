package io.scalecube.ipc;

import static io.scalecube.ipc.Event.Topic;

import io.scalecube.transport.Address;

import rx.Observable;
import rx.Subscription;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultEventStream implements EventStream {

  private final Subject<Event, Event> subject = PublishSubject.<Event>create().toSerialized();
  private final Subject<Event, Event> closeSubject = PublishSubject.<Event>create().toSerialized();

  private final ConcurrentMap<ChannelContext, Subscription> subscriptions = new ConcurrentHashMap<>();

  private final Function<Event, Event> eventMapper;

  public DefaultEventStream() {
    this(Function.identity());
  }

  public DefaultEventStream(Function<Event, Event> eventMapper) {
    this.eventMapper = eventMapper;
  }

  @Override
  public final void subscribe(ChannelContext channelContext) {
    // register cleanup process upfront
    channelContext.listenClose(this::onChannelContextClosed);

    // bind channelContext to this event stream
    Subscription subscription = channelContext.listen()
        .doOnUnsubscribe(() -> onChannelContextUnsubscribed(channelContext))
        .subscribe(this::onNext);

    // save subscription
    subscriptions.put(channelContext, subscription);
  }

  @Override
  public final void unsubscribe(Address address) {
    // full scan and filter then unsubscribe
    subscriptions.keySet().forEach(channelContext -> {
      if (address.equals(channelContext.getAddress())) {
        unsubscribe(channelContext);
      }
    });
  }

  @Override
  public final Observable<Event> listen() {
    return subject.onBackpressureBuffer().asObservable().map(eventMapper::apply);
  }

  @Override
  public final void close() {
    subject.onCompleted();
    closeSubject.onCompleted();
    // full scan and unsubscribe
    subscriptions.keySet().forEach(this::unsubscribe);
  }

  @Override
  public final void listenClose(Consumer<Void> onClose) {
    closeSubject.subscribe(event -> {
    }, throwable -> onClose.accept(null), () -> onClose.accept(null));
  }

  private void onNext(Event event) {
    subject.onNext(event);
  }

  private void onChannelContextClosed(ChannelContext channelContext) {
    unsubscribe(channelContext);
    subject.onNext(new Event.Builder(Topic.ChannelContextClosed, channelContext).build());
  }

  private void onChannelContextUnsubscribed(ChannelContext channelContext) {
    subject.onNext(new Event.Builder(Topic.ChannelContextUnsubscribed, channelContext).build());
  }

  private void unsubscribe(ChannelContext channelContext) {
    Subscription subscription = subscriptions.remove(channelContext);
    if (subscription != null) {
      subscription.unsubscribe();
    }
  }
}

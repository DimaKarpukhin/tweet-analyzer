package com.handson.twitter.services;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class AppKafkaReceiver {
    public static final String FINISH_PROCESSING = "FINISH_PROCESSING";
    EmitterProcessor<String> emitterProcessor;
    Disposable d;
    public reactor.kafka.receiver.KafkaReceiver<String,String> simpleConsumer(String topic) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProps)
                .commitInterval(Duration.ZERO)
                .commitBatchSize(0)
                .subscription(Collections.singleton(topic));
        reactor.kafka.receiver.KafkaReceiver<String,String> receiver =   reactor.kafka.receiver.KafkaReceiver.create(receiverOptions);
        return receiver;
    }
    public  Flux<String> listen(String topic) {
        emitterProcessor = EmitterProcessor.create();
        emitterProcessor.map(x->x);

        ConnectableFlux<ReceiverRecord<String, String>> connector = simpleConsumer(topic)
                .receive()
                .publish();

        Scheduler flowScheduler = Schedulers.newSingle("sample", true);

        CountDownLatch cdl = new CountDownLatch(1);
        connector.publishOn(flowScheduler)
                .subscribe((x) ->{
                    if (x.value().equals(FINISH_PROCESSING)){
                        System.out.println("inside finish");
                        emitterProcessor.onComplete();
                    }else {
                        System.out.println("adding next:" + x.value());
                        emitterProcessor.onNext(x.value());
                    }
                }, e -> cdl.countDown(), cdl::countDown);

        d = connector.connect(); // <- run kafka
        return emitterProcessor;
    }
    public void stopListen() {
        d.dispose();
    }

}

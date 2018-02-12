/**
 * Copyright 2018, Matthias Wessendorf
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.wessendorf.rx.kafka;

import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.subjects.PublishSubject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class KafkaStream<T> {

    final static ExecutorService executor = Executors.newFixedThreadPool(1);
    final Properties properties = new Properties();
    final KafkaConsumer<String, String> consumer;
    private final Logger logger = Logger.getLogger(KafkaStream.class.getName());
    private final String topic;

    private final ConsumerRecordsRunnable crr = new ConsumerRecordsRunnable();

    Flowable<ConsumerRecord<String,String>> consumerRecordFlowable;
    PublishSubject<ConsumerRecord<String,String>> publishSubject = PublishSubject.<ConsumerRecord<String, String>>create();




    public KafkaStream(final String topic) {
        this.topic = topic;
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "172.17.0.4:9092");
        properties.put(GROUP_ID_CONFIG, "rx-kafka"+ UUID.randomUUID().toString());
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer(properties);

        consumer.subscribe(Arrays.asList(topic));

        try {
            executor.submit(crr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public final Disposable subscribe(Consumer<? super ConsumerRecord<String, String>> onNext, Consumer<? super Throwable> onError, Action onComplete) {
        return publishSubject.subscribe(onNext, onError, onComplete);
    }

    private class ConsumerRecordsRunnable implements Runnable {

        @Override
        public void run() {

            logger.warning("Done w/ subscribing");

            boolean running = true;
            while (running) {
                final ConsumerRecords<String, String> records = consumer.poll(100);

                Flowable.fromIterable(records).subscribe(
                        record -> {
                            publishSubject.onNext(record);
                        },
                        error -> {
                            // fix me
                        },
                        () -> {
                            // we are done... - are we ?
                        });

            }


        }
    }

}

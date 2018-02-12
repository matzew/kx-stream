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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Runner {

    private static final DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

    public static void main(String[] args) {

        new KafkaStream<ConsumerRecord<String, String>>("demo-topic")
                .subscribe(
                        record -> {
                            System.out.println("We got this:  " + record.value() + ", time: " + dateFormat.format(new Date(record.timestamp())));
                        },
                        Throwable::printStackTrace,
                        () -> {
                            System.out.println("DONE!");
                        }
                );
    }
}

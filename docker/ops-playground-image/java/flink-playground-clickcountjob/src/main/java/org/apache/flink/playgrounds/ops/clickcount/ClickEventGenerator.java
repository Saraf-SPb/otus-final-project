/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.playgrounds.ops.clickcount;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.*;

import static org.apache.flink.playgrounds.ops.clickcount.ClickEventCount.WINDOW_SIZE;

/**
 * A generator which pushes {@link ClickEvent}s into a Kafka Topic configured via `--topic` and
 * `--bootstrap.servers`.
 *
 * <p> The generator creates the same number of {@link ClickEvent}s for all pages. The delay between
 * events is chosen such that processing time and event time roughly align. The generator always
 * creates the same sequence of events. </p>
 */
public class ClickEventGenerator {

    public static final int EVENTS_PER_WINDOW = 100;

    private static final List<String> pages = Arrays.asList("/help", "/index", "/shop", "/jobs", "/about", "/news");

    private static final List<String> trafficSource = Arrays.asList("google.com", "yandex.ru", "youtube.com", "vk.com");

    //this calculation is only accurate as long as pages.size() * EVENTS_PER_WINDOW divides the
    //window size
    public static final long DELAY = WINDOW_SIZE.toMilliseconds() / pages.size() / EVENTS_PER_WINDOW;

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String topic = params.get("topic", "input");

        Properties kafkaProps = createKafkaProperties(params);

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps);

        ClickIterator clickIterator = new ClickIterator();

        while (true) {

            ProducerRecord<byte[], byte[]> record = new ClickEventSerializationSchema(topic).serialize(
                    clickIterator.next(),
                    null);

            producer.send(record);

            Thread.sleep(DELAY);
        }
    }

    private static Properties createKafkaProperties(final ParameterTool params) {
        String brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return kafkaProps;
    }

    static class ClickIterator {

        private static final int NUMBER_OF_USERS = 5;
        private static final int MAX_DURATION = 200;
        private Map<String, Long> nextTimestampPerKey;
        private int nextPageIndex;
        private int nextTrafficSourceIndex;

        ClickIterator() {
            nextTimestampPerKey = new HashMap<>();
            nextPageIndex = 0;
            nextTrafficSourceIndex = 0;
        }

        ClickEvent next() {
            String page = nextPage();
            long userId = nextUserId();
            long duration = nextDuration();
            String trafficSource = nextTrafficSource();
            return new ClickEvent(nextTimestamp(page), page, userId, duration, trafficSource);
        }

        private Date nextTimestamp(String page) {
            long nextTimestamp = nextTimestampPerKey.getOrDefault(page, 0L);
            nextTimestampPerKey.put(page, nextTimestamp + WINDOW_SIZE.toMilliseconds() / EVENTS_PER_WINDOW);
            return new Date(nextTimestamp);
        }

        private String nextPage() {
            String nextPage = pages.get(nextPageIndex);
            Random rnd = new Random();
            nextPageIndex = rnd.nextInt(pages.size());
//            if (nextPageIndex == pages.size() - 1) {
//                nextPageIndex = 0;
//            } else {
//                nextPageIndex++;
//            }
            return nextPage;
        }

        private long nextUserId() {
            Random rnd = new Random();
            return 2013000000 + rnd.nextInt(NUMBER_OF_USERS);

        }

        private long nextDuration() {
            Random rnd = new Random();
            return rnd.nextInt(MAX_DURATION);

        }

        private String nextTrafficSource() {
            String nextTrafficSource = trafficSource.get(nextTrafficSourceIndex);
            Random rnd = new Random();
            nextTrafficSourceIndex = rnd.nextInt(trafficSource.size());
//            if (nextTrafficSourceIndex == trafficSource.size() - 1) {
//                nextTrafficSourceIndex = 0;
//            } else {
//                nextTrafficSourceIndex++;
//            }
            return nextTrafficSource;
        }

    }
}

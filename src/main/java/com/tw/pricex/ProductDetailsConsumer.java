package com.tw.pricex;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pulsar.PulsarSourceBuilder;


public class ProductDetailsConsumer {

    static String PULSAR_TOPIC = "topic1";
    static String SUBSCRIPTION = "subscription1";
    static String PULSAR_URL = "pulsar://localhost:6650";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        PulsarSourceBuilder<String> builder = PulsarSourceBuilder
                .builder(new SimpleStringSchema())
                .serviceUrl(PULSAR_URL)
                .topic(PULSAR_TOPIC)
                .subscriptionName(SUBSCRIPTION);
        SourceFunction<String> src = builder.build();
        DataStream<String> words = env.addSource(src);

        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("/Users/in-cbe-santhosh/Documents/ws/cbe-de/pricex-product-consumer/output/products/"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .build();
        words.addSink(sink);

        env.execute("Product Details Consumer");
    }
}


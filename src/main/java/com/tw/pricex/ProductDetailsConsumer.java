package com.tw.pricex;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pulsar.PulsarSourceBuilder;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;


public class ProductDetailsConsumer {

    static String PULSAR_TOPIC = "topic1";
    static String SUBSCRIPTION = "subscription1";
    static String PULSAR_URL = "pulsar://apachepulsar-standalone:6650";

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
//                .forRowFormat(new Path("/output/products/"),
//                        new SimpleStringEncoder<String>("UTF-8"))
                .forRowFormat(new Path("/output/products/"), (Encoder<String>) (element, stream) -> {
                    PrintStream out = new PrintStream(stream);
                    out.println(element);
                })
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(TimeUnit.SECONDS.toSeconds(10))
//                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
//                                .withMaxPartSize(1024)
//                                .build())
                .build();
        words.addSink(sink);

//        words.print();

        env.execute("Product Details Consumer");
    }
}


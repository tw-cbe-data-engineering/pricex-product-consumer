package com.tw.pricex;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pulsar.PulsarSourceBuilder;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;


public class WordCountStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        PulsarSourceBuilder<String> builder = PulsarSourceBuilder
                .builder(new SimpleStringSchema())
                .serviceUrl("pulsar://localhost:6650")
                .topic("my-topic")
                .subscriptionName("subscription1");
        SourceFunction<String> src = builder.build();
        DataStream<String> words = env.addSource(src);

        DataStream<WordWithCount> wc = words
                .flatMap((FlatMapFunction<String, WordWithCount>) (word, collector) -> {
                    collector.collect(new WordWithCount(word, 1));
                })

                .returns(WordWithCount.class)
                .keyBy("word")
                .timeWindow(seconds(5))
                .reduce((ReduceFunction<WordWithCount>) (c1, c2) ->
                        new WordWithCount(c1.word, c1.count + c2.count));

        wc.print().setParallelism(1);
        env.execute("Wordcount");
    }


    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long l) {
            this.word=word;
            this.count=l;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}

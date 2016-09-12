package net.vanlaer.tim;

//https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/DirectKafkaWordCount.scala

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class SparkStart {

    public static void main(String[] args) throws InterruptedException {

        Map<String, String> props = Maps.newHashMap();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("spark.streaming.kafka.maxRatePerPartition", "10000");


        SparkConf conf = new SparkConf().setAppName("bla").setMaster("local[4]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Seconds.apply(2));
        streamingContext.checkpoint("/tmp");

        Set<String> topics = Sets.newHashSet("test");
        JavaPairInputDStream<String, String> directKafkaStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        props,
                        topics
                );

        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

        /*directKafkaStream.transformToPair(
                (Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>) rdd -> {
                    OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    offsetRanges.set(offsets);
                    return rdd;
                }
        ).foreachRDD(
                (VoidFunction<JavaPairRDD<String, String>>) stringStringJavaPairRDD -> {
                    for (OffsetRange o : offsetRanges.get()) {
                        System.out.println(
                                o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
                        );
                    }
                }
        );*/


       /* directKafkaStream
                .map((Function<Tuple2<String, String>, String>) v1 -> v1._2)
                .countByWindow(Duration.apply(2000), Duration.apply(2000))
                .print();*/



        directKafkaStream
                .map((Function<Tuple2<String, String>, Integer>) v1 -> Integer.valueOf(v1._2))
                .reduce((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2)
                .print();






        /*directKafkaStream
                .map((Function<Tuple2<String, String>, String>) v1 -> v1._2)
                .flatMap((FlatMapFunction<String, String>) s -> Splitter.on(" ").split(s).iterator());
*/

        /*val lines = directKafkaStream.map(_._2)
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x = > (x, 1L)).reduceByKey(_ + _)
        wordCounts.print()*/


        // Start the computation
        streamingContext.start();
        streamingContext.awaitTermination();


    }

}

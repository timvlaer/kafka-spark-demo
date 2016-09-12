bin/kafka-console-consumer.sh --zookeeper localhost:2181 \
             --topic test \
             --from-beginning \
             --formatter kafka.tools.DefaultMessageFormatter \
             --property print.key=true \
             --property print.value=true \
             --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
             --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
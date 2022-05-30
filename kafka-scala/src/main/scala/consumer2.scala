import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.KafkaConsumer
import collection.JavaConverters._
import java.time.Duration
import java.util
object TestConsumer{
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:29092")
    props.setProperty("enable.auto.commit", "false")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer(props)
    consumer.assign(List(new TopicPartition("storage-topic",3)).asJava)
   // consumer.subscribe(List("test").asJava)

    while(true){
      val buffer = new util.ArrayList[AnyRef]
      val records = consumer.poll(Duration.ofMillis(100))
      import collection.JavaConverters._
      for (record <- records.asScala) {
        buffer.add(record)
      }
      if(buffer.size() > 0) {
        print(buffer)
        Thread.sleep(100)
      }
    }
  }
}
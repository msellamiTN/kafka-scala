
import java.util.{Collections, Properties}
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
object KafkaConsumerSubscribeApp extends App {

  val props:Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers","localhost:29092")
  props.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer") 
  props.put("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
  //props.put("enable.auto.commit", "true")
  //props.put("auto.commit.interval.ms", "1000")
  //props.put("auto.offset.reset", "earliest")
    props.put("auto.offset.reset", "latest")
  val consumer = new KafkaConsumer(props)
  val topics = List("storage-topic")
  println("Topic: " + topics + "listener")
  try {
    consumer.subscribe(topics.asJava)
    while (true) {
      println("begin get topic data")
      val records = consumer.poll(1000)
      consumer.seekToBeginning(consumer.assignment())
      
      for (record <- records.asScala) {
        println("Topic: " + record.topic() + 
                 ",Key: " + record.key() +  
                 ",Value: " + record.value() +
                 ", Offset: " + record.offset() + 
                 ", Partition: " + record.partition())
      }
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
  }
}

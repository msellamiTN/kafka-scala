import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.AdminClient
import java.util.Properties;
object TopicCreation extends App {  
  val props:Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers","localhost:29092")
  props.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer") 
  props.put("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val adminClient = AdminClient.create(props)
try {
      val newTopic = new NewTopic("storage-topic", 1, 1.toShort)
     val results =adminClient.createTopics(java.util.Collections.singletonList(newTopic))
    } finally {
      adminClient.close()
    }
            
 
 }
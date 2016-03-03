package net.fawad.learningkafka

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}
import scala.collection.JavaConversions._
import resource.managed

import scala.concurrent.Future
import scala.util.Random

object Main extends App {
  val props = new Properties()
  props.putAll(Map("bootstrap.servers" -> "localhost:9092", "group.id" -> "learningkafka"))
  val consumers = 5
  val sleepDuration = 30000
  import scala.concurrent.ExecutionContext.Implicits.global
  for (producer <- managed(new KafkaProducer[String, String](props, new StringSerializer, new StringSerializer))) {
    val futures = for (c <- Range(0, consumers))
      yield Future {
        for (consumer <- managed(new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer))) {
          consumer.subscribe(List("learningkafka"))
          while (true) {
            val messageFromTopic = consumer.poll(30000)
            for (m <- messageFromTopic) {
              println(s"Consumer ${c}: Message: ${m.value()}")
            }
            Thread.sleep(sleepDuration)
          }
        }
      }
    while(true){
      val producerSleep = sleepDuration/consumers
      producer.send(new ProducerRecord[String, String]("learningkafka", s"foo${Random.nextInt()}", s"bar${Random.nextInt()}"))
      Thread.sleep(producerSleep)
    }
  }
}
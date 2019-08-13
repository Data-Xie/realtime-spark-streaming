package com.atguigu.qzpoint.consummer



import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RegisterConsummer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("registerConsummer")

    //5秒消费一次
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //kafka 参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "register_topic"
    val group = "bigdata"

    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )

    val dStream = KafkaUtils.createDirectStream[String,StringDecoder](ssc,kafkaParams,Set(topic))


  }
}

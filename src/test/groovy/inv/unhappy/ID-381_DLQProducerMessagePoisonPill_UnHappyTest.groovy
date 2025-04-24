import org.slf4j.*
import java.util.*
import java.text.DateFormat
import java.text.SimpleDateFormat

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.RecordMetadata

def topic_dlq = context.expand('${#Project#topic_dlq}')
def bootstrap_servers = context.expand('${#Project#bootstrap_servers}')
def schema_registry_url = context.expand('${#Project#schema_registry_url}')
def trust_store_location = context.expand('${#Project#trust_store_location}')
def key_serializer = context.expand('${#Project#key_serializer}')
def value_serializer = context.expand('${#Project#value_serializer}')
def security_protocol = context.expand('${#Project#security_protocol}')
def sasl_mechanism = context.expand('${#Project#sasl_mechanism}')
def credentials_source = context.expand('${#Project#credentials_source}')
def auth_user_info_producer = context.expand('${#Project#auth_user_info_producer}')
def jaas_config_producer = context.expand('${#Project#jaas_config_producer}')

Properties props_producer  = new Properties()
props_producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)
props_producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, key_serializer)
props_producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, value_serializer)
props_producer.put("security.protocol", security_protocol)
props_producer.put("sasl.mechanism", sasl_mechanism)
props_producer.put("basic.auth.credentials.source", credentials_source)
props_producer.put("basic.auth.user.info", auth_user_info_producer)
props_producer.put("sasl.jaas.config", jaas_config_producer)
props_producer.put("ssl.truststore.location", trust_store_location)
props_producer.put("ssl.truststore.password", "changeit")
props_producer.put("schema.registry.url", schema_registry_url)
props_producer.put("schema.registry.ssl.truststore.location", trust_store_location)
props_producer.put("schema.registry.ssl.truststore.password", "changeit")
props_producer.put("auto.register.schemas", false)
props_producer.put("specific.avro.reader", true)

ProducerRecord<Object, Object> record = new ProducerRecord<>(topic_dlq, null, null)

KafkaProducer producer = new KafkaProducer(props_producer)

producer.send(record)

producer.flush()
producer.close()

log.info("Poison pill was sent on dlq topic: " + topic_dlq + " ... with success")
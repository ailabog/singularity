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

import org.apache.kafka.clients.consumer.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords

import org.assertj.core.api.SoftAssertions

//Producer and Consumer variables
def topic_producer = context.expand('${#Project#topic_producer}')
def topic_consumer = context.expand('${#Project#topic_consumer}')
def bootstrap_servers = context.expand('${#Project#bootstrap_servers}')
def schema_registry_url = context.expand('${#Project#schema_registry_url}')
def trust_store_location = context.expand('${#Project#trust_store_location}')
def key_serializer = context.expand('${#Project#key_serializer}')
def key_deserializer = context.expand('${#Project#key_deserializer}')
def value_serializer = context.expand('${#Project#value_serializer}')
def value_deserializer = context.expand('${#Project#value_deserializer}')
def security_protocol = context.expand('${#Project#security_protocol}')
def sasl_mechanism = context.expand('${#Project#sasl_mechanism}')
def credentials_source = context.expand('${#Project#credentials_source}')
def auth_user_info_producer = context.expand('${#Project#auth_user_info_producer}')
def jaas_config_producer = context.expand('${#Project#jaas_config_producer}')
def auth_user_info_consumer = context.expand('${#Project#auth_user_info_consumer}')
def jaas_config_consumer = context.expand('${#Project#jaas_config_consumer}')

def currentDateComplete = "DUMMY"
def dateCreated = "DUMMY"
def eventType= "DUMMY"
def eventSubtype = "DUMMY"
def eventCorrelationId = "DUMMY"
def eventSourceDescription = "DUMMY"
def  eventSource = "DUMMY"
def  documentType = "DUMMY"
def  businessArea = "DUMMY"
def  batchNoPfx = "DUMMY"
def  objectStore = "DUMMY"
def  mimeType = "DUMMY"
def  systemAddedID = "DUMMY"
def docClass = "DUMMY"
def GUID = "DUMMY"
SoftAssertions softly = new SoftAssertions()

//Kafka Consumer variablesto setup the props: groupId, brokers, topic, schema
def unique_group_id = "automation_doc_events_consumer" + currentDateComplete

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

def avroSchema = '{}'

Schema.Parser parser = new Schema.Parser()
Schema schemaInv = parser.parse(avroSchema)

GenericRecord avroRecord = new GenericData.Record(schemaInv)
//  log.info(avroRecord.toString())

GenericRecord eventHeader = new GenericData.Record(schemaInv.getField("eventheader").schema())
eventHeader.put("eventType", eventType)
eventHeader.put("eventSubtype", eventSubtype)
eventHeader.put("eventDateTime", currentDateComplete)
eventHeader.put("eventGeneratedDateTime", currentDateComplete)
eventHeader.put("eventCorrelationId", eventCorrelationId)
eventHeader.put("eventRequestId", eventCorrelationId)
eventHeader.put("eventSourceDescription", eventSourceDescription)
eventHeader.put("eventSource", eventSource)
eventHeader.put("metadata", new HashMap<>())

GenericRecord eventBody = new GenericData.Record(schemaInv.getField("eventBody").schema())

eventBody.put("Document_Type", documentType)
eventBody.put("BusinessArea", businessArea)
eventBody.put("Batch_No_Pfx", batchNoPfx)
eventBody.put("ObjectStore", objectStore)
eventBody.put("MimeType", mimeType)
eventBody.put("SystemAddedID", systemAddedID)
eventBody.put("DateCreated", dateCreated)
eventBody.put("DocClass", docClass)
eventBody.put("GUID", GUID)
eventBody.put("MajorVersion", "1")
eventBody.put("MinorVersion", "0")

//propertiesList implementation
List<GenericRecord> propertiesList = new ArrayList<>()

Schema propertiesListSchema = schemaInv.getField("eventBody").schema().getField("propertiesList").schema().getElementType()
GenericRecord propertyRecord = new GenericData.Record(propertiesListSchema)

propertyRecord.put("name", "PropertyName")
propertyRecord.put("value", "PropertyValue")
propertyRecord.put("type", "PropertyType")
propertyRecord.put("multiValue", "PropertyMultiValue")
propertyRecord.put("multiList", new GenericData.Array<>(propertiesListSchema.getField("multiList").schema(), Arrays.asList("Value1", "Value2")))
propertiesList.add(propertyRecord)
eventBody.put("propertiesList", propertiesList)

avroRecord.put("eventheader", eventHeader)
avroRecord.put("eventBody", eventBody)


ProducerRecord<Object, Object> recordProducer = new ProducerRecord<>(topic_producer, null, avroRecord)

KafkaProducer producer = new KafkaProducer(props_producer)

RecordMetadata metadata = producer.send(recordProducer).get()
log.info("Producer record with eventCorrelationId: " + eventCorrelationId.toString())

producer.flush()
producer.close()

log.info("Message sent successfully to topic" + metadata.topic() + " " +
        "partition: " + metadata.partition() + " "+ "offset: " + metadata.offset())

//Kafka Consumer Setup
Properties props_consumer  = new Properties()
props_consumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)
props_consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, key_deserializer)
props_consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, value_deserializer)
props_consumer.put(ConsumerConfig.GROUP_ID_CONFIG, unique_group_id)
props_consumer.put("security.protocol", security_protocol)
props_consumer.put("sasl.mechanism", sasl_mechanism)
props_consumer.put("basic.auth.credentials.source", credentials_source)
props_consumer.put("basic.auth.user.info", auth_user_info_consumer)
props_consumer.put("sasl.jaas.config", jaas_config_consumer)
props_consumer.put("ssl.truststore.location", trust_store_location)
props_consumer.put("ssl.truststore.password", "changeit")
props_consumer.put("schema.registry.url", schema_registry_url)
props_consumer.put("schema.registry.ssl.truststore.location", trust_store_location)
props_consumer.put("schema.registry.ssl.truststore.password", "changeit")
props_consumer.put("auto.offset.reset", "latest")

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props_consumer)
consumer.subscribe(Arrays.asList(topic_consumer))

ConsumerRecords<String, String> records = consumer.poll(1000)

for(ConsumerRecords<String, String> record : records) {
    GenericRecord avroRecordConsum = record.value()
    log.info("Current  record value: " + record.value().toString())
    def evIdConsumer = avroRecordConsum.get("eventHeader.eventCorrelationId").toString()

    softly.assertThat(eventCorrelationId).isNotEqualTo(evIdConsumer)
}
softly.assertAll()
consumer.close()

log.info("Validating the Consumer fields against the fields produced with success, message doesn't reach the Consumer")
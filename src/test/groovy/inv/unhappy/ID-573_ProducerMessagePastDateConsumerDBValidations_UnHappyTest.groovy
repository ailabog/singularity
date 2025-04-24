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
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.sql.ResultSet

import org.assertj.core.api.SoftAssertions

//Producer variables
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
def auth_user_info_consumer = context.expand('${#Project#auth_user_info_consumer}')
def jaas_config_producer = context.expand('${#Project#jaas_config_producer}')
def jaas_config_consumer = context.expand('${#Project#jaas_config_consumer}')
def mysql_db = context.expand('${#Project#mysql_db}')
def username_mysql = context.expand('${#Project#username_mysql}')
def password_mysql = context.expand('${#Project#password_mysql}')
def queryMessage = 'SELECT * FROM message WHERE  JSON_EXTRACT(payload, "$.content.sharedData.originalevId")= ?'
def applicationId = "DC-ORCH"
def messageType = "push"

Date now = new Date()
SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
String currentDateComplete = date.format(now)

UUID generateUUID = UUID.randomUUID()

SimpleDateFormat inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
inputFormat.setTimeZone(TimeZone.getTimeZone("EDT"))
Calendar calendar = Calendar.getInstance()
calendar.setTime(now)
calendar.add(Calendar.DAY_OF_MONTH, -20)
Date pastDate = calendar.getTime()
String datePast = inputFormat.format(pastDate)

def eventType= ""
def eventSubtype = ""
def evId = generateUUID.toString()
def eventSourceDescription = ""
def  eventSource = ""
def  docT = ""
def  businessArea = ""
def  batchNoPfx = ""
def  objectStore = ""
def  mimeType = "application/pdf"
def  systemAddedID = ""
def docClass = ""
def GUID = '{' + generateUUID.toString() + '}'
def uniqueGroupId = "automation_doc_events_consumer" + currentDateComplete
SoftAssertions softly = new SoftAssertions()

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
eventHeader.put("evId", evId)
eventHeader.put("eventRequestId", evId)
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
eventBody.put("DateCreated", datePast)
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
log.info("Producer record with evId: " + evId.toString())

producer.flush()
producer.close()

log.info("Message sent successfully to topic" + metadata.topic() + " " +
        "partition: " + metadata.partition() + " "+ "offset: " + metadata.offset())

//Kafka Consumer Setup
Properties props_consumer  = new Properties()
props_consumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)
props_consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, key_deserializer)
props_consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, value_deserializer)
props_consumer.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueGroupId)
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
    String evIdConsumer = avroRecordConsum.get("eventHeader.evId").toString()

    softly.assertThat(evId).isNotEqualTo(evIdConsumer)
}
softly.assertAll()
consumer.close()

log.info("No message found on Kafka Consumer with date:" + datePast)

//DB execution
Connection conn = DriverManager.getConnection(mysql_db, username_mysql, password_mysql)
PreparedStatement statement = conn.prepareStatement(queryMessage)
statement.setString(1, evId)

ResultSet rs = statement.executeQuery()

if(!rs.next()){

    log.info("There is no message in DB")
} else {
    log.info("There is something wrong and there is  a message in DB")
}

conn.close()

log.info("No message found in the database with document type: " + docT)
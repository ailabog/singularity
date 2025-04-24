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

import okhttp3.*
import org.json.JSONArray
import org.json.JSONObject

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.sql.ResultSet
import org.assertj.core.api.SoftAssertions

//Producer variables
def topic_producer = context.expand('${#Project#topic_producer}')
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

Date now = new Date()
SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
String currentDateComplete = date.format(now)

UUID generateUUID = UUID.randomUUID()

SimpleDateFormat inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
inputFormat.setTimeZone(TimeZone.getTimeZone("EDT"))
String dateCreated = inputFormat.format(now)

def eventType= ""
def eventSubtype = ""
def eventCorrelationId = generateUUID.toString()
def eventSourceDescription = ""
def  eventSource = ""
def  documentType = ""
def  businessArea = ""
def  batchNoPfx = ""
def  objectStore = ""
def  mimeType = "application/pdf"
def  systemAddedID = ""
def  transactionID = ""
def  primaryHoldingID = ""
def  adminSystem = ""
def docClass = ""
def GUID = '{' + generateUUID.toString() + '}'

//Kafka Consumer variablesto setup the props: groupId, brokers, topic, schema
def unique_group_id = "automation_doc_events_consumer" + currentDateComplete
def topic_consumer = context.expand('${#Project#topic_consumer}')
def key_deserializer = context.expand('${#Project#key_deserializer}')
def value_deserializer = context.expand('${#Project#value_deserializer}')
def auth_user_info_consumer = context.expand('${#Project#auth_user_info_consumer}')
def jaas_config_consumer = context.expand('${#Project#jaas_config_consumer}')

//MSs calls
//Define necessary variables for the Agreement Customer/Customer Ids MS
def agreementKey = ""
def url_agr_cst = context.expand('${#Project#url_agr_cst_micro}') +"/" +agreementKey
def usr_agr = context.expand('${#Project#usr_agr}')
def pswd_agr = context.expand('${#Project#pswd_agr}')

def usr_cstIds = context.expand('${#Project#usr_cstIds}')
def pswd_cstIds = context.expand('${#Project#pswd_cstIds}')
def expectedRole = ""

OkHttpClient client = new OkHttpClient()
SoftAssertions softly = new SoftAssertions()

//MySQL variables
def mysql_db = context.expand('${#Project#mysql_db}')
def username_mysql = context.expand('${#Project#username_mysql}')
def password_mysql = context.expand('${#Project#password_mysql}')
def queryMessage = 'SELECT * FROM message WHERE JSON_EXTRACT(payload, "$.content.sharedData.origEvId")= ?'
def applicationIdExpected = ""
def messageType = ""

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


String avroSchema = '{}'
Schema.Parser parser = new Schema.Parser()
Schema schemaInv = parser.parse(avroSchema)

GenericRecord avroRecord = new GenericData.Record(schemaInv)
//  log.info(avroRecord.toString())

GenericRecord eventHeader = new GenericData.Record(schemaInv.getField("eventheader").schema())
eventHeader.put("eventType", eventType)
eventHeader.put("eventSubtype", eventSubtype)
eventHeader.put("eventDateTime", currentDateComplete)
eventHeader.put("eventGeneratedDateTime", currentDateComplete)
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

log.info("Ebill message produced by Kafka Producer: " + eventHeader.toString() + eventBody.toString())

ProducerRecord<Object, Object> recordProducer = new ProducerRecord<>(topic_producer, null, avroRecord)

KafkaProducer producer = new KafkaProducer(props_producer)

RecordMetadata metadata = producer.send(recordProducer).get()

producer.flush()
producer.close()

log.info("Message sent successfully to topic" + metadata.topic() + " " +
        "partition: " + metadata.partition() + " "+ "offset: " + metadata.offset())

log.info("Kafka Producer Primary_Holding_ID is:" + eventBody.get("Primary_Holding_ID1"))
log.info("AdminSystem  is:" + eventBody.get("AdminSystem"))
String agreementKeyProducer = (eventBody.get("Primary_Holding_ID1") + eventBody.get("AdminSystem")).toString()


// Kafka Consumer Setup
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

KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props_consumer)
consumer.subscribe(Arrays.asList(topic_consumer))

ConsumerRecords<String, GenericRecord> records = consumer.poll(1000)

for(ConsumerRecords<String, GenericRecord> record : records) {
    GenericRecord avroRecordConsum = record.value()
    log.info("Current  record value: " + record.value().toString())
    String eventCorrelationIdConsumer = avroRecordConsum.get("eventHeader.eventCorrelationId").toString()

    if(eventCorrelationId.equals(eventCorrelationIdConsumer)) {
        softly.assertThat(avroRecordConsum.get("eventHeader.eventType").toString()).IsEqualTo(eventType)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventSubtype").toString()).isEqualTo(eventSubtype)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventDateTime").toString()).isEqualTo(currentDateComplete)
        softly.assertThat(avroRecordConsum.get("eventHeader.eventGeneratedDateTime").toString()).isEqualTo(currentDateComplete)
        softly.assertThat(avroRecordConsum.get("eventBody.Document_Type").toString()).isEqualTo(documentType)
        softly.assertThat(avroRecordConsum.get("eventBody.BusinessArea").toString()).isEqualTo(businessArea)
        softly.assertThat(avroRecordConsum.get("eventBody.Batch_No_Pfx").toString()).isEqualTo(batchNoPfx)
        softly.assertThat(avroRecordConsum.get("eventBody.ObjectStore").toString()).isEqualTo(objStore)
        softly.assertThat(avroRecordConsum.get("eventBody.MimeType").toString()).isEqualTo(mimeType)
        softly.assertThat(avroRecordConsum.get("eventBody.SystemAddedID").toString()).isEqualTo(systemAddedID)
        softly.assertThat(avroRecordConsum.get("eventBody.Transaction_ID").toString()).isEqualTo(transactionID)
        softly.assertThat(avroRecordConsum.get("eventBody.AdminSystem").toString()).isEqualTo(adminSystem)
    }
    long offset = record.offset()
    log.info("Consumed record at offset: " + offset)
}
softly.assertAll()
consumer.close()

log.info("Validating the Consumer fields against the fields produced... with success, messages is sent on Consumer side")


Request request_agreementKey = new Request.Builder().url(url_agr_cst).header("Authorization", Credentials.basic(usr_agr, pswd_agr)).header("Content-Type", "application/json").build()

Response response_agreementKey = client.newCall(request_agreementKey).execute()
def responseBody_agreementKey = response_agreementKey.body().string()
int statusCode_agreementKey = response_agreementKey.code()

JSONObject jsonRspAgrCst = new JSONObject(responseBody_agreementKey)

JSONArray agreements = jsonRspAgrCst.getJSONArray("agreements")
JSONObject customer = agreements.getJSONObject(0).getJSONArray("agreementCustomers").getJSONObject(0)

JSONArray agreementsArray = jsonRspAgrCst.getJSONArray("agreements")
def agreementKeyMS = agreementsArray.getJSONObject(0).getString("agreementKey")

def roleType = customer.getString("roleType")
def mbrGUIDAgrtCst = customer.getString("memberGUID")

//Customer id Call
def url_customer_id = context.expand('${#Project#schemaInv}') + mbrGUIDAgrtCst

Request request_customerId = new Request.Builder().url(url_customer_id).header("Authorization", Credentials.basic(usr_cstIds, pswd_cstIds)).header("Content-Type", "application/json").build()

Response rsp_cstId = client.newCall(request_customerId).execute()
String responseBody_customeId= rsp_cstId.body().string()
int statusCode_cstId = rsp_cstId.code()

JSONObject jsonResponseCustomerId = new JSONObject(responseBody_customeId)

JSONArray customers = jsonResponseCustomerId.getJSONArray("customers")
String  mbrGUIDCstId = customers.getJSONObject(0).getString("memberGUID")

if(roleType.equals(expectedRole) && (mbrGUIDAgrtCst).equals(mbrGUIDCstId)) {

    log.info ("Agreement key from producer is: " + agreementKeyProducer)
    log.info("Agreement key from Agreement customer MS is: "   + agreementKeyMS)
}

//DB validations
Connection conn = DriverManager.getConnection(mysql_db, username_mysql, password_mysql)
PreparedStatement statement = conn.prepareStatement(queryMessage)
statement.setString(1, eventCorrelationId)

ResultSet rs = statement.executeQuery()

if(rs.next()){
    def type = rs.getString("type")
    def source = rs.getString("source")
    def payload = rs.getString("payload")
    def received_date = rs.getString("received_date")

    JSONObject jsonPayload = new JSONObject(payload)
    JSONObject sharedData = jsonPayload.getJSONObject("content").getJSONObject("sharedData")

    def origEvId = sharedData.getString("origEvId")
    def applicationId = jsonPayload.getString("applicationId")
    def overwriteMessageType = sharedData.getString("overwriteMessageType")
    def origEvInitDesc = sharedData.getString("origEvInitDesc")
    def origAppId = sharedData.getString("origAppId")
    def no = sharedData.getString("no")
    def userId = sharedData.getString("userId")
    def fileName = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("parameters").getString("fileName")
    def sound = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("data").getString("sound")
    def templateType = jsonPayload.getJSONArray("items").getJSONObject(0).getString("templateType")
    def sourceType = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getString("sourceType")
    JSONArray onsuccess = jsonPayload.getJSONObject("triggers").getJSONArray("onsuccess")
    def DbmessageType = jsonPayload.getString("messageType")

    if (origEvId.equals(eventCorrelationId)) {

        softly.assertThat(applicationId).isEqualTo(applicationIdExpected)
        softly.assertThat(overwriteMessageType).isEqualTo(messageType)
        softly.assertThat(origEvInitDesc).isEqualTo(eventSourceDescription)
        softly.assertThat(no).isEqualTo(primaryHoldingID)
        softly.assertThat(originalEventDateTime).isEqualTo(currentDateComplete)
        softly.assertThat(no).isEqualTo(primaryHoldingID)
        softly.assertThat(templateType).isEqualTo("")
        softly.assertThat(sourceType).isEqualTo("")
        softly.assertThat(DbmessageType).isEqualTo(messageType)
        softly.assertAll()
    }
    else {
        log.info("Something must be really wrong with this data")
    }
}
conn.close()

log.info("Validating the db fields against Kafka fields produced with success")


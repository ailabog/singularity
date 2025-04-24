import org.slf4j.*
import java.util.*
import java.text.DateFormat
import java.text.SimpleDateFormat

import org.json.JSONArray
import org.json.JSONObject

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.RecordMetadata

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
SoftAssertions softly = new SoftAssertions()

Date now = new Date()
SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
String currentDateComplete = date.format(now)

UUID generateUUID = UUID.randomUUID()

SimpleDateFormat inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
inputFormat.setTimeZone(TimeZone.getTimeZone("EDT"))
String dateCreated = inputFormat.format(now)

def eventType= ""
def eventSubtype = ""
def evId = generateUUID.toString()
def evSrcDesc = ""
def  eventSource = ""
def  documentType = ""
def  businessArea = ""
def  batchNoPfx = ""
def  objectStore = ""
def  mimeType = "application/pdf"
def  systemAddedID = ""
def  transactionID = ""

def  primaryHoldingID = ""
def suplimentalHoldingID = ""
def  adminSystem = ""
def docClass = ""
def GUID = '{' + generateUUID.toString() + '}'

//MySQL variables
def mysql_db = context.expand('${#Project#mysql_db}')
def username_mysql = context.expand('${#Project#username_mysql}')
def password_mysql = context.expand('${#Project#password_mysql}')
def queryMessage = 'SELECT * FROM table where col1 =?'
def appIdEx = ""
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
eventHeader.put("evId", evId)
eventHeader.put("eventRequestId", evId)
eventHeader.put("evSrcDesc", evSrcDesc)
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

GenericRecord primaryObj = new GenericData.Record(propertiesListSchema)

//Populate fields for Primary Holding ID
primaryObj.put("name", "")
primaryObj.put("value", "")
primaryObj.put("type", "String")
primaryObj.put("multiValue", "1")
primaryObj.put("multiList", new GenericData.Array<>(propertiesListSchema.getField("multiList").schema(), Arrays.asList("")))


propertiesList.add(primaryObj)


GenericRecord suplObj = new GenericData.Record(propertiesListSchema)

//Populate fileds for Suplimental Holding ID
suplObj.put("name", "")
suplObj.put("value", "VA")
suplObj.put("type", "String")
suplObj.put("multiValue", "1")
suplObj.put("multiList", new GenericData.Array<>(propertiesListSchema.getField("multiList").schema(), Arrays.asList("VA")))

propertiesList.add(suplObj)

//Add propertiesList to eventBody
eventBody.put("propertiesList", propertiesList)

avroRecord.put("eventheader", eventHeader)
avroRecord.put("eventBody", eventBody)

log.info("Inv message produced by Kafka Producer: " + eventHeader.toString() + eventBody.toString())

ProducerRecord<Object, Object> recordProducer = new ProducerRecord<>(topic_producer, null, avroRecord)

KafkaProducer producer = new KafkaProducer(props_producer)

RecordMetadata metadata = producer.send(recordProducer).get()
log.info("Producer record with evId: " + evId.toString())

producer.flush()
producer.close()

log.info("Message sent successfully to topic" + metadata.topic() + " " +
        "partition: " + metadata.partition() + " "+ "offset: " + metadata.offset())

log.info("Kafka Producer Primary_Holding_ID is:" + eventBody.get(""))
log.info("AdminSystem  is:" + eventBody.get("AdminSystem"))

//DB validations
Connection conn = DriverManager.getConnection(mysql_db, username_mysql, password_mysql)
PreparedStatement statement = conn.prepareStatement(queryMessage)
statement.setString(1, evId)

ResultSet rs = statement.executeQuery()

if(rs.next()){
    def type = rs.getString("type")
    def source = rs.getString("source")
    def payload = rs.getString("payload")
    def received_date = rs.getString("received_date")

    JSONObject jsonPayload = new JSONObject(payload)
    JSONObject sharedData = jsonPayload.getJSONObject("content").getJSONObject("sharedData")

    def originalevId = sharedData.getString("originalevId")

    def originalEventRequestID = sharedData.getString("originalEventRequestID")
    def originalevSrcDesc = sharedData.getString("originalevSrcDesc")
    def userId = sharedData.getString("userId")
    def fileName = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("parameters").getString("fileName")
    def sound = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("data").getString("sound")
    def templateType = jsonPayload.getJSONArray("items").getJSONObject(0).getString("templateType")
    def sourceType = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getString("sourceType")
    JSONArray onsuccess = jsonPayload.getJSONObject("triggers").getJSONArray("onsuccess")
    def DbmessageType = jsonPayload.getString("messageType")

    if (originalevId.equals(evId)) {

        softly.assertThat(appId).isEqualTo(appIdEx)
        softly.assertThat(originalevSrcDesc).isEqualTo(evSrcDesc)
        softly.assertThat(originalEventRequestID).isEqualTo(evId)
        softly.assertThat(originalEventInitiator).isEqualTo(appId)
        softly.assertThat(fileName).isEqualTo("")
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

log.info("Validating the db fields against Kafka fields produced  with success")

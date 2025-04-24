import org.slf4j.*
import java.util.*
import java.text.DateFormat
import java.text.SimpleDateFormat
import org.json.JSONObject
import org.json.JSONArray

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

//Kafka Producer variables
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

//MySQL variables
def mysql_db = context.expand('${#Project#mysql_db}')
def username_mysql = context.expand('${#Project#username_mysql}')
def password_mysql = context.expand('${#Project#password_mysql}')
def queryMessage = 'SELECT * FROM table where JSON_EXTRACT(payload, "$.content.sharedData.origEvId")= ?'
def applicationIdExpected = ""
def messageType = "b"

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
def  adminSystem = ""
def docClass = ""
def GUID = '{' + generateUUID.toString() + '}'

Properties props  = new Properties()

props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, key_serializer)
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, value_serializer)
props.put("security.protocol", security_protocol)
props.put("sasl.mechanism", sasl_mechanism)
props.put("basic.auth.credentials.source", credentials_source)
props.put("basic.auth.user.info", auth_user_info_producer)
props.put("sasl.jaas.config", jaas_config_producer)
props.put("ssl.truststore.location", trust_store_location)
props.put("ssl.truststore.password", "changeit")
props.put("schema.registry.url", schema_registry_url)
props.put("schema.registry.ssl.truststore.location", trust_store_location)
props.put("schema.registry.ssl.truststore.password", "changeit")
props.put("auto.register.schemas", false)
props.put("specific.avro.reader", true)

String avroSchema = '{}'
Schema.Parser parser = new Schema.Parser()
Schema schemaInv = parser.parse(avroSchema)

GenericRecord avroRecord = new GenericData.Record(schemaInv)

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

ProducerRecord<Object, Object> record = new ProducerRecord<>(topic_producer, null, avroRecord)

KafkaProducer producer = new KafkaProducer(props);

RecordMetadata metadata = producer.send(record).get()

producer.flush()
producer.close()

log.info("Message sent successfully to topic" + metadata.topic() + " " +
        "partition: " + metadata.partition() + " "+ "offset: " + metadata.offset())

//DB validations
Connection conn = DriverManager.getConnection(mysql_db, username_mysql, password_mysql)
PreparedStatement statement = conn.prepareStatement(queryMessage)
statement.setString(1, evId)

ResultSet rs = statement.executeQuery()

if(rs.next()){
    String type = rs.getString("type")
    String source = rs.getString("source")
    String payload = rs.getString("payload")
    String received_date = rs.getString("received_date")

    log.info("Validating the DB payload:")

    JSONObject jsonPayload = new JSONObject(payload)
    JSONObject sharedData = jsonPayload.getJSONObject("content").getJSONObject("sharedData")

    def origEvId = sharedData.getString("origEvId")
    def applicationId = jsonPayload.getString("applicationId")
    def originalevSrcDesc = sharedData.getString("originalevSrcDesc")
    def originalEventSource = sharedData.getString("originalEventSource")
    def originalEventInitiator = sharedData.getString("originalEventInitiator")
    def originalEventDateTime = sharedData.getString("originalEventDateTime")

    def fileName = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("parameters").getString("fileName")
    def sound = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("data").getString("sound")
    def templateType = jsonPayload.getJSONArray("items").getJSONObject(0).getString("templateType")
    def sourceType = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getString("sourceType")
    JSONArray onsuccess = jsonPayload.getJSONObject("triggers").getJSONArray("onsuccess")
    def DbmessageType = jsonPayload.getString("messageType")

    log.info("Validating the db fields against the  fields produced... with success")

    if (origEvId.equals(evId)) {

        softly.assertThat(applicationId).isEqualTo(applicationIdExpected)
        softly.assertThat(overwriteMessageType).isEqualTo(messageType)
        softly.assertThat(originalEventInitiatorDescription).isEqualTo(evSrcDesc)
        softly.assertThat(policyNumber).isEqualTo(primaryHoldingID)
        softly.assertThat(originalEventRequestID).isEqualTo(evId)
        softly.assertThat(originalEventDateTime).isEqualTo(currentDateComplete)
        softly.assertThat(policyNumber).isEqualTo(primaryHoldingID)
        softly.assertThat(originalEventInitiator).isEqualTo(applicationId)
        softly.assertThat(sourceType).isEqualTo("")
        softly.assertThat(DbmessageType).isNotEqualTo(messageType)
        softly.assertAll()
    }
    else {
        log.info("Something must be really wrong with this data")
    }
}
conn.close()
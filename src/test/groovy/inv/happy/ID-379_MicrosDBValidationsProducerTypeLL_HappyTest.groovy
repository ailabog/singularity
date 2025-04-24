import okhttp3.*
import org.json.JSONArray
import org.json.JSONObject

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

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.sql.ResultSet
import org.assertj.core.api.SoftAssertions

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
def evId = generateUUID.toString()
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

//MSs calls
//Define necessary variables for the Agreement Customer/Customer Ids MS
def key = "123"
def url_agr_cst = context.expand('${#Project#url_agr_cst_micro}') +"/" +key
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
def queryMessage = 'SELECT * FROM mo where JSON_EXTRACT(payload, "$.content.sharedData.origEvId")= ?'
def applicationIdExpected = ""
def messageType = ""

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

def avroSchema = '{}'
Schema.Parser parser = new Schema.Parser()
Schema schemaInv = parser.parse(avroSchema)

GenericRecord avroRecord = new GenericData.Record(schemaInv)

GenericRecord eventHeader = new GenericData.Record(schemaInv.getField("eventheader").schema())
eventHeader.put("eventType", eventType)
eventHeader.put("eventSubtype", eventSubtype)
eventHeader.put("eventDateTime", currentDateComplete)
eventHeader.put("eventGeneratedDateTime", currentDateComplete)
eventHeader.put("eventCorrelationId", evId)
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
eventBody.put("DateCreated", dateCreated)
eventBody.put("DocClass", docClass)
eventBody.put("GUID", GUID)
eventBody.put("MajorVersion", "1")
eventBody.put("MinorVersion", "0")

//propertiesList implementation
List<GenericRecord> propertiesList = new ArrayList<>()

Schema propertiesListSchema = avroSchemaEbill.getField("eventBody").schema().getField("propertiesList").schema().getElementType()
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

def agreementKeyProducer = (eventBody.get("Primary_Holding_ID1") + eventBody.get("AdminSystem")).toString()

//Agreement key call
Request request_agr = new Request.Builder().url(url_agr_cst).header("Authorization", Credentials.basic(usr_agr, pswd_agr)).header("Content-Type", "application/json").build()

Response response_agreementKey = client.newCall(request_agr).execute()
def responseBody_agreementKey = response_agreementKey.body().string()
int statusCode_agreementKey = response_agreementKey.code()

JSONObject jsonResponseAgreementCustomer = new JSONObject(responseBody_agreementKey)

JSONArray agreements = jsonResponseAgreementCustomer.getJSONArray("agreements")
JSONObject customer = agreements.getJSONObject(0).getJSONArray("agreementCustomers").getJSONObject(0)

JSONArray agreementsArray = jsonResponseAgreementCustomer.getJSONArray("agreements")
def agreementKeyMS = agreementsArray.getJSONObject(0).getString("agreementKey")

def roleType = customer.getString("roleType")
def memberGUIDAgreementCustomer = customer.getString("memberGUID")

//Save memberGUID)
def url_customer_id = context.expand('${#Project#url_cst_id_micro}') + memberGUIDAgreementCustomer

Request request_customerId = new Request.Builder().url(url_customer_id).header("Authorization", Credentials.basic(usr_cstIds, pswd_cstIds)).header("Content-Type", "application/json").build()

Response response_customerId = client.newCall(request_customerId).execute()
String responseBody_customeId= response_customerId.body().string()
int statusCode_customerId = response_customerId.code()

JSONObject jsonResponseCustomerId = new JSONObject(responseBody_customeId)

JSONArray customers = jsonResponseCustomerId.getJSONArray("customers")
String  memberGUIDCustomerId = customers.getJSONObject(0).getString("memberGUID")

if(roleType.equals(expectedRole) && (memberGUIDAgreementCustomer).equals(memberGUIDCustomerId)) {

    log.info ("Agreement key from producer is: " + agreementKeyProducer)
    log.info("Agreement key from Agr c MS is: "   + agreementKeyMS)
}

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

    log.info("Validating the DB payload:")

    JSONObject jsonPayload = new JSONObject(payload)
    JSONObject sharedData = jsonPayload.getJSONObject("content").getJSONObject("sharedData")

    def originalEventCorrelationID = sharedData.getString("originalEventCorrelationID")
    def applicationId = jsonPayload.getString("applicationId")
    def overwriteMessageType = sharedData.getString("overwriteMessageType")
    def originalEventInitiatorDescription = sharedData.getString("originalEventInitiatorDescription")
    def originalApplicationId = sharedData.getString("originalApplicationId")
    def originalEventSource = sharedData.getString("originalEventSource")
    def originalEventInitiator = sharedData.getString("originalEventInitiator")
    def originalEventDateTime = sharedData.getString("originalEventDateTime")
    def userId = sharedData.getString("userId")
    def fileName = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("parameters").getString("fileName")
    def sound = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("data").getString("sound")
    def templateType = jsonPayload.getJSONArray("items").getJSONObject(0).getString("templateType")
    def sourceType = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getString("sourceType")
    JSONArray onsuccess = jsonPayload.getJSONObject("triggers").getJSONArray("onsuccess")
    def DbmessageType = jsonPayload.getString("messageType")

    if (originalEventCorrelationID.equals(eventCorrelationId)) {

        softly.assertThat(applicationId).isEqualTo(applicationIdExpected)
        softly.assertThat(overwriteMessageType).isEqualTo(messageType)
        softly.assertThat(originalEventInitiatorDescription).isEqualTo(eventSourceDescription)
        softly.assertThat(originalEventDateTime).isEqualTo(currentDateComplete)
        softly.assertThat(originalEventInitiator).isEqualTo(applicationId)
        softly.assertThat(DbmessageType).isEqualTo(messageType)
        softly.assertAll()
    }
    else {
        log.info("Something must be really wrong with this data")
    }
}
conn.close()

log.info("Validating the db fields against the Kafka fields produced with success")
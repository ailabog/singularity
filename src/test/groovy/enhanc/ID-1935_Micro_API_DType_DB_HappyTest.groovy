import okhttp3.*
import okhttp3.MediaType
import org.json.JSONArray
import org.json.JSONObject
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Random
import org.assertj.core.api.SoftAssertions
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.sql.ResultSet

//Define necessary variables for  POST Micro/API e
def url_micro_e = context.expand('${#Project#url_micro_e}')
def username_e = context.expand('${#Project#username_micro_e}')
def password_e = context.expand('${#Project#password_micro_e}')
def url_api_e = context.expand('${#Project#url_api_e}')
def username_api = context.expand('${#Project#username_api_e}')
def password_api = context.expand('${#Project#password_api_e}')

//MySQL variables
def mysql_db = context.expand('${#Project#mysql_db}')
def username_mysql = context.expand('${#Project#username_mysql}')
def password_mysql = context.expand('${#Project#password_mysql}')
def queryMessage = 'SELECT * FROM message WHERE id = ?'
def applicationIdExpected = ""
def messageType = ""

Date now = new Date()
SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
String currentDateComplete = date.format(now)

UUID generateUUID = UUID.randomUUID()

SimpleDateFormat inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
inputFormat.setTimeZone(TimeZone.getTimeZone("EDT"))
String dateCreated = inputFormat.format(now)

def eventid = generateUUID.toString()
def userId = generateUUID.toString()
def appId  = ""
def no  = ""
def appIdType = ""
def overwriteMessageType = ""
def originalEventInitiatorDescription = ""
def messageFormat = "TEXT"
def sourceType = ""

// template
def eTemplate = """
 {
   
}
"""
log.info("e Template: " +eTemplate)
RequestBody  ePayload = RequestBody.create(MediaType.parse("application/json"), eTemplate)

OkHttpClient client = new OkHttpClient()
Request request_micro_e = new Request.Builder().url(url_micro_e).header("Authorization", Credentials.basic(username_e, password_e)).header("Content-Type", "application/json").post(ePayload).build()

Response response_micro_e= client.newCall(request_micro_e).execute()
def responseBody_micro_e= response_micro_e.body().string()
int statusCode_micro_e = response_micro_e.code()

SoftAssertions softly = new SoftAssertions()

softly.assertThat(statusCode_micro_e).isEqualTo(200)
softly.assertAll()

log.info("Response for POST Micro e :" + responseBody_micro_e)
log.info("Status code for POST Micro e:" + statusCode_micro_e)

//Validate the repsonse
JSONObject postMicroeRsp = new JSONObject(responseBody_micro_e)
def transTypeRsp = postMicroeRsp.getString("transType")
def transGUIDMicroeRsp = postMicroeRsp.getString("transGUID")
def idMicroeRsp = postMicroeRsp.getString("id")
def transExecDateTSMicroeRsp = postMicroeRsp.getString("transExecDateTS")
def msgTblIdentMicroeRsp = postMicroeRsp.getLong("msgTblIdent")

JSONArray msgArray = postMicroeRsp.getJSONArray("messages")
JSONObject firstMsg = msgArray.getJSONObject(0)

def msgDesc = firstMsg.getString("msgDesc")
def msgType = firstMsg.getString("msgType")
def msgCode = firstMsg.getString("msgCode")

softly.assertThat(msgType).isEqualTo("SUCCESS")
softly.assertThat(msgCode).isEqualTo("1")
softly.assertThat(msgDesc).isEqualTo("Message Process Begun - request written to database")
softly.assertThat(transTypeRsp).isEqualTo("WriteMessage")
softly.assertAll()

// template
def eTemplateAPI = """
 {
 }
"""
log.info("e Template: " +eTemplateAPI)
RequestBody  ePayloadAPI = RequestBody.create(MediaType.parse("application/json"), eTemplateAPI)

Request request_API_e = new Request.Builder().url(url_api_e).header("Authorization", Credentials.basic(username_api, password_api)).header("Content-Type", "application/json").post(ePayloadAPI).build()

Response response_API_e= client.newCall(request_API_e).execute()
String responseBody_API_e= response_API_e.body().string()
int statusCode_API_e = response_API_e.code()

softly.assertThat(statusCode_API_e).isEqualTo(200)
softly.assertAll()

log.info("Response for POST API e :" + responseBody_API_e)
log.info("Status code for POST API e:" + statusCode_API_e)

//Validate the repsonse
JSONObject postAPIeRsp = new JSONObject(responseBody_API_e)
String transTypeAPIeRsp = postAPIeRsp.getString("transType")
String transGUIDAPIeRsp = postAPIeRsp.getString("transGuid")
String idApiERsp = postAPIeRsp.getString("id")
String transExecDateTSAPIeRsp = postAPIeRsp.getString("transExecDateTS")
String msgTblIdentAPIeRsp = postAPIeRsp.getLong("messageTableIdent")

JSONArray msgArrayAPIe = postAPIeRsp.getJSONArray("messages")
JSONObject firstMsgAPIe = msgArrayAPIe.getJSONObject(0)

String msgDescAPIe = firstMsgAPIe.getString("description")
String msgTypeAPIe = firstMsgAPIe.getString("type")
String msgCodeAPIe = firstMsgAPIe.getString("code")

softly.assertThat(msgTypeAPIe).isEqualTo("SUCCESS")
softly.assertThat(msgCodeAPIe).isEqualTo("1")
softly.assertThat(msgDescAPIe).isEqualTo("Message Process Begun - request written to database")
softly.assertThat(transTypeAPIeRsp).isEqualTo("eespondenceAPI")
softly.assertAll()

//DB validations
Connection conn = DriverManager.getConnection(mysql_db, username_mysql, password_mysql)
PreparedStatement statement = conn.prepareStatement(queryMessage)
statement.setLong(1, msgTblIdentMicroeRsp)

ResultSet rs = statement.executeQuery()

if(rs.next()){
    def typeDB= rs.getString("type")
    def formatDB= rs.getString("format")
    def sourceDB = rs.getString("source")
    def payloadDB = rs.getString("payload")
    def received_dateDB = rs.getString("received_date")
    def identDB = rs.getString("ident")
    def statusDB = rs.getString("status")
    def errorMessageDB = rs.getString("error_message")

    JSONObject jsonPayload = new JSONObject(payloadDB)
    JSONObject sharedData = jsonPayload.getJSONObject("content").getJSONObject("sharedData")

    def originalEventidB = sharedData.getString("originalEventid")
    def applicationIdDB = jsonPayload.getString("applicationId")
    def overwriteMessageTypeDB = sharedData.getString("overwriteMessageType")
    def originalEventInitiatorDescriptionDB = sharedData.getString("originalEventInitiatorDescription")
    def originalApplicationIdDB = sharedData.getString("originalApplicationId")
    def noDB = sharedData.getString("policyNumber")
    def originalEventRequestIDDB = sharedData.getString("originalEventRequestID")
    def originalEventSourceDescriptionDB = sharedData.getString("originalEventSourceDescription")
    def originalEventSourceDB = sharedData.getString("originalEventSource")
    def originalEventInitiatorDB = sharedData.getString("originalEventInitiator")
    def originalEventDateTimeDB = sharedData.getString("originalEventDateTime")

    def DbmessageType = jsonPayload.getString("messageType")

    if (msgTblIdentMicroeRsp.equals(identDB)) {

        softly.assertThat(applicationIdDB).isEqualTo(applicationIdExpected)
        softly.assertThat(overwriteMessageTypeDB).isEqualTo(messageType)
        softly.assertThat(originalEventidB).isEqualTo(eventid)
        softly.assertThat(noDB).isEqualTo(no)
        softly.assertThat(originalEventSourceDescriptionDB).isEqualTo(originalEventInitiatorDescription)
        softly.assertThat(originalEventSourceDB).isEqualTo("")
        softly.assertThat(noDB).isEqualTo(no)
        softly.assertThat(originalEventInitiatorDB).isEqualTo(appId)
        softly.assertThat(typeDB).isEqualTo(overwriteMessageType)
        softly.assertThat(DbmessageType).isEqualTo(messageType)
        softly.assertAll()
    }
    else {
        log.info("Something must be really wrong with this data")
    }
    conn.close()
}
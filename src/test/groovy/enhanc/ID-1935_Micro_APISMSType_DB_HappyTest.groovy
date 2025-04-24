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

Date now = new Date()
SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
String currentDateComplete = date.format(now)

UUID generateUUID = UUID.randomUUID()

SimpleDateFormat inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
inputFormat.setTimeZone(TimeZone.getTimeZone("EDT"))
String dateCreated = inputFormat.format(now)

def eventCorrelationId = generateUUID.toString()
def username = ""
def password = ""
def userId = generateUUID.toString()
def appId  = ""
def originalEventInitiatorDescription = ""
def messageFormat = "TEXT"
def messageType = ""
def email = ""
def phoneNo = ""

// template
def eTemplate = """
 {
  
}
"""
log.info("e Template: " +eTemplate)
RequestBody  ePayload = RequestBody.create(MediaType.parse("application/json"), eTemplate)

OkHttpClient client = new OkHttpClient()
Request request_micro_e = new Request.Builder().url(url_micro_e).header("Authorization", Credentials.basic(username, password)).header("Content-Type", "application/json").post(ePayload).build()

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
def correlationIdMicroeRsp = postMicroeRsp.getString("correlationId")
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
log.info("e Template: " +eTemplate)
RequestBody  ePayloadAPI = RequestBody.create(MediaType.parse("application/json"), eTemplateAPI)

Request request_api_e = new Request.Builder().url(url_api_e).header("Authorization", Credentials.basic(username_api, password_api)).header("Content-Type", "application/json").post(ePayloadAPI).build()

Response response_api_e= client.newCall(request_api_e).execute()
def responseBody_api_e= response_api_e.body().string()
int statusCode_api_e = response_api_e.code()

softly.assertThat(statusCode_api_e).isEqualTo(200)
softly.assertAll()

log.info("Response for POST api e :" + responseBody_api_e)
log.info("Status code for POST api e:" + statusCode_api_e)

//Validate the repsonse
JSONObject postApieRsp = new JSONObject(responseBody_api_e)
def transTypeRspApie = postApieRsp.getString("transType")
def transGUIDApieRsp = postApieRsp.getString("transGuid")
def correlationIdApieRsp = postApieRsp.getString("correlationid")
def transExecDateTSApieRsp = postApieRsp.getString("transExecDateTS")
def msgTblIdentApieRsp = postApieRsp.getLong("messageTableIdent")

JSONArray msgArrayAPIe = postApieRsp.getJSONArray("messages")
JSONObject firstMsgAPIe = msgArrayAPIe.getJSONObject(0)

def msgDescAPIe = firstMsgAPIe.getString("description")
def msgTypeAPIe = firstMsgAPIe.getString("type")
def msgCodeAPIe = firstMsgAPIe.getString("code")

softly.assertThat(msgTypeAPIe).isEqualTo("SUCCESS")
softly.assertThat(msgCodeAPIe).isEqualTo("1")
softly.assertThat(msgDescAPIe).isEqualTo("Message Process Begun - request written to database")
softly.assertThat(transTypeRspApie).isEqualTo("eespondenceAPI")
softly.assertAll()

//DB validations
Connection conn = DriverManager.getConnection(mysql_db, username_mysql, password_mysql)
PreparedStatement statement = conn.prepareStatement(queryMessage)
statement.setLong(1, msgTblIdentApieRsp)

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

    def applicationIdDB = jsonPayload.getString("applicationId")
    def originalEventInitiatorDescriptionDB = sharedData.getString("originalEventInitiatorDescription")
    def originalApplicationIdDB = sharedData.getString("originalApplicationId")
    def originalEventRequestIDDB = sharedData.getString("originalEventRequestID")
    def originalEventSourceDescriptionDB = sharedData.getString("originalEventSourceDescription")
    def originalEventDateTimeDB = sharedData.getString("originalEventDateTime")

    def DbmessageType = jsonPayload.getString("messageType")

    if (msgTblIdentApieRsp.equals(identDB)) {

        softly.assertThat(originalApplicationIdDB).isEqualTo(appId)
        softly.assertThat(originalEventRequestIDDB).isEqualTo(eventCorrelationId)
        softly.assertThat(originalEventSourceDescriptionDB).isEqualTo(appId)
        softly.assertThat(DbmessageType).isEqualTo(messageType)
        softly.assertAll()
    }
    else {
        log.info("Something must be really wrong with this data")
    }
    conn.close()
}
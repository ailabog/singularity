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

//Define necessary variables for  POST API E
def url_api_e = context.expand('${#Project#url_api_e}')
def username_e = context.expand('${#Project#username_api_e}')
def password_e = context.expand('${#Project#password_api_e}')

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

def evId = generateUUID.toString()
def userId = generateUUID.toString()
def appId = "TEST"
def msgT = "o"
def msgF = "pdf"
def srcT = ""
def key = "123"

// template
def eTemplate = """
 {
 
}

"""
log.info("ETemplate: " +eTemplate)
RequestBody  ePayload = RequestBody.create(MediaType.parse("application/json"), eTemplate)

OkHttpClient client = new OkHttpClient()
Request request_API_e = new Request.Builder().url(url_api_e).header("Authorization", Credentials.basic(username_e, password_e)).header("Content-Type", "application/json").post(ePayload).build()

Response response_API_e= client.newCall(request_API_e).execute()
def responseBody_API_e= response_API_e.body().string()
int statusCode_API_e = response_API_e.code()

SoftAssertions softly = new SoftAssertions()

softly.assertThat(statusCode_API_e).isEqualTo(200)
softly.assertAll()

log.info("Response for POST API E :" + responseBody_API_e)
log.info("Status code for POST API E:" + statusCode_API_e)

//Validate the repsonse
JSONObject postAPIEcorrRsp = new JSONObject(responseBody_API_er)
def transTypeRsp = postAPIEcorrRsp.getString("transType")
def transGUIDAPIERsp = postAPIEcorrRsp.getString("transGuid")
def correlationIdAPIERsp = postAPIEcorrRsp.getString("correlationid")
def transExecDateTSAPIERsp = postAPIEcorrRsp.getString("transExecDateTS")
def msgTblIdentAPIERsp = postAPIEcorrRsp.getLong("messageTableIdent")

JSONArray msgArray = postAPIEcorrRsp.getJSONArray("messages")
JSONObject firstMsg = msgArray.getJSONObject(0)

def msgDesc = firstMsg.getString("description")
def msgType = firstMsg.getString("type")
def msgCode = firstMsg.getString("code")

softly.assertThat(msgType).isEqualTo("SUCCESS")
softly.assertThat(msgCode).isEqualTo("1")
softly.assertThat(msgDesc).isEqualTo("Message Process Begun - request written to database")
softly.assertAll()

//DB validations
Connection conn = DriverManager.getConnection(mysql_db, username_mysql, password_mysql)
PreparedStatement statement = conn.prepareStatement(queryMessage)
statement.setLong(1, msgTblIdentAPIERsp)

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

    def internalCorrelationIdDB = sharedData.getString("internalCorrelationId")

    def DbmessageType = jsonPayload.getString("messageType")

    if (msgTblIdentAPIEcorrRsp.equals(identDB)) {

        softly.assertThat(applicationIdDB).isEqualTo(appId)
        softly.assertThat(typeDB).isEqualTo(messageType)
        softly.assertThat(DbmessageType).isEqualTo(messageType)
        softly.assertAll()
    }
    else {
        log.info("Something must be really wrong with this data")
    }

    log.info("Validating the db fields against the  fields produced for: " + "type: "  + typeDB + " , " + "format:  " + formatDB + " , " + "source: " + sourceDB + " , " + "received date: " + received_dateDB+  " , " + "payloadDB:" +payloadDB +"... with success")

    conn.close()
}
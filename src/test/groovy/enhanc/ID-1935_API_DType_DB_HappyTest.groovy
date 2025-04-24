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

//Define necessary variables for  POST API e
def url_api_e = context.expand('${#Project#url_api_e}')
def username_e = context.expand('${#Project#username_api_e}')
def password_e = context.expand('${#Project#password_api_e}')

//MySQL variables
def mysql_db = context.expand('${#Project#mysql_db}')
def username_mysql = context.expand('${#Project#username_mysql}')
def password_mysql = context.expand('${#Project#password_mysql}')
def queryMessage = 'SELECT * FROM message WHERE id = ?'
def appIdEx = ""
def messageType = ""

Date now = new Date()
SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
String currentDateComplete = date.format(now)

UUID generateUUID = UUID.randomUUID()

SimpleDateFormat inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
inputFormat.setTimeZone(TimeZone.getTimeZone("EDT"))
String dateCreated = inputFormat.format(now)

def evId = generateUUID.toString()

def userId = generateUUID.toString()
def appId  = ""
def no  = "00"
def appIdType = ""
def msgT = ""
def origEvInitDesc = ""
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
Request request_API_e = new Request.Builder().url(url_api_e).header("Authorization", Credentials.basic(username_e, password_e)).header("Content-Type", "application/json").post(ePayload).build()

Response response_API_e= client.newCall(request_API_e).execute()
def responseBody_API_e= response_API_e.body().string()
int statusCode_API_e = response_API_e.code()

SoftAssertions softly = new SoftAssertions()

//softly.assertThat(statusCode_API_e).isEqualTo(200)
//softly.assertAll()

log.info("Response for POST API e :" + responseBody_API_e)
log.info("Status code for POST API e:" + statusCode_API_e)

//Validate the repsonse
JSONObject postAPIeRsp = new JSONObject(responseBody_API_e)
def transTypeRsp = postAPIeRsp.getString("transType")
def transGUIDAPIeRsp = postAPIeRsp.getString("transGuid")
def idAPIeRsp = postAPIeRsp.getString("id")
def transExecDateTSAPIeRsp = postAPIeRsp.getString("transExecDateTS")
def msgTblidAPIeRsp = postAPIeRsp.getLong("messageTableid")

JSONArray msgArray = postAPIeRsp.getJSONArray("messages")
JSONObject firstMsg = msgArray.getJSONObject(0)

def msgDesc = firstMsg.getString("description")
def msgType = firstMsg.getString("type")
def msgCode = firstMsg.getString("code")

softly.assertThat(msgType).isEqualTo("SUCCESS")
softly.assertThat(msgCode).isEqualTo("1")
softly.assertThat(msgDesc).isEqualTo("Message Process Begun - request written to database")
softly.assertThat(transTypeRsp).isEqualTo("eeAPI")
softly.assertAll()

//DB validations
Connection conn = DriverManager.getConnection(mysql_db, username_mysql, password_mysql)
PreparedStatement statement = conn.prepareStatement(queryMessage)
statement.setLong(1, msgTblidAPIeRsp)

ResultSet rs = statement.executeQuery()

if(rs.next()){
    def typeDB= rs.getString("type")
    def formatDB= rs.getString("format")
    def sourceDB = rs.getString("source")
    def payloadDB = rs.getString("payload")
    def received_dateDB = rs.getString("received_date")
    def idDB = rs.getString("id")
    def statusDB = rs.getString("status")
    def errorMessageDB = rs.getString("error_message")

    JSONObject jsonPayload = new JSONObject(payloadDB)
    JSONObject sharedData = jsonPayload.getJSONObject("content").getJSONObject("sharedData")

    def origEvIdDB = sharedData.getString("origEvId")
    def appIdDB = jsonPayload.getString("appId")
    def msgTDB = sharedData.getString("msgT")
    def origEvInitDescDB = sharedData.getString("origEvInitDesc")
    def originalappIdDB = sharedData.getString("originalApplicationId")
    def noDB = sharedData.getString("no")


    def DbmessageType = jsonPayload.getString("messageType")

    if (msgTblidAPIeRsp.equals(idDB)) {

        softly.assertThat(appIdDB).isEqualTo(appIdEx)
        softly.assertThat(overwriteMessageTypeDB).isEqualTo(messageType)
        softly.assertThat(origEvId).isEqualTo(evId)
        softly.assertThat(noDB).isEqualTo(no)
        softly.assertThat(typeDB).isEqualTo(msgT)
        softly.assertThat(DbmessageType).isEqualTo(messageType)
        softly.assertAll()
    }
    else {
        log.info("Something must be really wrong with this data")
    }

    conn.close()
}
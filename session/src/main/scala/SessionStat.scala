import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object SessionStat {


    def main(args: Array[String]): Unit = {

        val jsonString: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
        val taskParam: JSONObject = JSONObject.fromObject(jsonString)
        val taskUUID: String = UUID.randomUUID.toString

        val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
        val actionRDD: RDD[UserVisitAction] = getOriActionRDD(sparkSession, taskParam)
        val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map((item: UserVisitAction) => (item.session_id, item))
        val session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()
        session2GroupActionRDD.cache()
        val sessionId2FullInfoRDD: RDD[(String, String)] = getSessionFullInfo(sparkSession, session2GroupActionRDD)
        getSessionFilteredRDD(taskParam, sessionId2FullInfoRDD)
    }

    def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
        val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
        val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

        val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'"
        import sparkSession.implicits._
        sparkSession.sql(sql).as[UserVisitAction].rdd
    }

    def getSessionFullInfo(sparkSession: SparkSession, session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {
        val userId2AggrInfoRDD: RDD[(Long, String)] = session2GroupActionRDD.map {
            case (sessionId, iterableAction) => {
                var userId = -1L;
                var startTime: Date = null
                var endTime: Date = null
                var stepLength: Long = -1;
                val searchWords: StringBuilder = new StringBuilder()
                val clickCategories: StringBuilder = new StringBuilder()

                for (action: UserVisitAction <- iterableAction) {
                    if (userId == -1) userId = action.user_id
                    val actionTime: Date = DateUtils.parseTime(action.action_time)
                    if (startTime == null || startTime.after(actionTime)) startTime = actionTime
                    if (endTime == null || endTime.before(actionTime)) endTime = actionTime

                    val searchKeyword: String = action.search_keyword
                    if (StringUtils.isNotEmpty(searchKeyword) && !searchWords.toString().contains(searchKeyword)) {
                        searchWords.append(searchKeyword + ",")
                    }

                    val clickCategoryId: Long = action.click_category_id
                    if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)) {
                        clickCategories.append(clickCategoryId + ",")
                    }
                    stepLength += 1
                }
                val searchKw = StringUtils.trimComma(searchWords.toString)
                val clickCg = StringUtils.trimComma(clickCategories.toString)

                val visitLength = (endTime.getTime - startTime.getTime) / 1000

                val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
                    Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
                    Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
                    Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
                    Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
                    Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
                (userId, aggrInfo)
            }
        }

        val sqlString = "select * from user_info"
        import sparkSession.implicits._
        val userInfoRDD: RDD[(Long, UserInfo)] = sparkSession.sql(sqlString).as[UserInfo].rdd.map((item: UserInfo) => (item.user_id, item))
        val sessionID2FullInfoRDD: RDD[(String, String)] = userId2AggrInfoRDD.join(userInfoRDD).map {
            case (userId, (aggrInfo, userInfo)) => {
                val age: Int = userInfo.age
                val professional: String = userInfo.professional
                val sex: String = userInfo.sex
                val city: String = userInfo.city

                val fullInfo = aggrInfo + "|" +
                    Constants.FIELD_AGE + "=" + age + "|" +
                    Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
                    Constants.FIELD_SEX + "=" + sex + "|" +
                    Constants.FIELD_CITY + "=" + city
                val sessionId: String = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
                (sessionId, fullInfo)
            }
        }
        sessionID2FullInfoRDD
    }

    def getSessionFilteredRDD(taskParam: JSONObject, sessionId2FullInfoRDD: RDD[(String, String)]) = {
        val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
        val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
        val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
        val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
        val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
        val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
        val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)


        var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
            (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
            (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
            (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
            (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
            (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
            (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

        if (filterInfo.endsWith("\\|"))
            filterInfo = filterInfo.substring(0, filterInfo.length - 1)
        sessionId2FullInfoRDD.filter {
            case (sessionId, fullInfo) => {
                var success = true

                if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    success = false
                } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
                    success = false
                } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
                    success = false
                } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
                    success = false
                } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
                    success = false
                } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
                    success = false
                }
                success
            }
        }
    }

}

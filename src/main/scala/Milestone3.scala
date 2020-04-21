import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

object Milestone3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Milestone3")
    val sc = SparkContext.getOrCreate(conf)

    val resourceManager = args(0)
    //val appLogs = args(1)
    val startId = args(1)
    val endId = args(2)


    val lines = sc.textFile(resourceManager)

    /*
    Extract infos from the log:
    After an analysis of the log, I've noticed 4 regex that would capture the most important informations
    in the log.
    * PatternStartTimeAttempt serves to capture the start time of each application attempt
    * userAppId uses the keywords java:activateApplications(911) and application_1580812675067_() to find
      the link between each applications and users in the right timestamp
    * Similarly, containers info contains keywords FiCaSchedulerNode.java:allocateContainer(169) then i simply
      Regex the line and capture important informations such as attempt, id, containerID and host
     */

    val patternStartTimeAttempt = """(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3}).+appattempt_1580812675067_(\d+)_0000(\d+).+from\sALLOCATED\sto\sLAUNCHED.+""".r
    val userAppId = """(.+java:activateApplications\(911\)\)).+application_1580812675067_(\d+)\sfrom\suser:\s(\w+).+""".r
    val container = """(.+FiCaSchedulerNode.java:allocateContainer\(169\)\)).+container_e02_1580812675067_(\d+)_(\d+)_(\d+).+host.(\w+.\w+.\w+.\w+).+""".r
    val endDateStatus = """(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3}).+appattempt_1580812675067_(\d+)_0000(\d+).+from\sFINAL_SAVING\sto\s(\w+).+""".r
    val exitCode = """(.+\(RMAppAttemptImpl.java:rememberTargetTransitionsAndStoreState\(1412\)\)).+appattempt_1580812675067_(\d+)_0000(\d+).+exit status: (-?\d+)""".r

    /*
    These functions just check if a line match previous regex, and extract tuples so that we end up with RDD of tuples
    */
    def parseLogContainers(line: String): (String, Int, Int, String) = {
      val container(logMessage, appId, attempt, containerID, host) = line
      (appId, attempt.toInt, containerID.toInt, host)
    }
    def parseLogStart(line: String): (String, Int, String) = {
      val patternStartTimeAttempt(date, appId, attempt) = line
      (appId, attempt.toInt, date)
    }
    def parseLogEnd(line: String): (String, Int, String, String) = {
      val endDateStatus(date, appId, appAttempt, finalState) = line
      (appId, appAttempt.toInt, date, finalState)
    }
    def parseLogExitCode(line: String): (String, Int, Int) = {
      val exitCode(logMessage, appId, appAttempt, code) = line
      (appId, appAttempt.toInt, code.toInt)
    }
    def parseLogUser(line: String): (String, String) = {
      val userAppId(logMessage, appId, user) = line
      (appId, user)
    }

    /*
    These 4 lists allows me to separate the data into groups. I simply filter each lines and map according to
    Each function above, to obtain tuples. I finally group those tuples so that making a join will be possible
    later on.
    */
    val containers = lines.filter(l => l.matches(container.toString()))
              .mapPartitions(p => p.toList.map(l => parseLogContainers(l)).iterator)
              .groupBy(pair => (pair._1, pair._2))

    val startTimes = lines.filter(l => l.matches(patternStartTimeAttempt.toString()))
              .mapPartitions(p => p.toList.map(l => parseLogStart(l)).iterator)
              .map{case (appId, appAttempt, date) => (appId, appAttempt) -> date}

    val appUsers = lines.filter(l => l.matches(userAppId.toString()))
              .mapPartitions(p => p.toList.map(l => parseLogUser(l)).iterator).distinct

    val exitCodes = lines.filter(l => l.matches(exitCode.toString()))
              .map(l => parseLogExitCode(l))
              .map{case (appId, appAttempt, code) => (appId, appAttempt) -> code}

    val endStatus = lines.filter(l => l.matches(endDateStatus.toString()))
              .map(l => parseLogEnd(l))
              .map{case (appId, appAttempt, date, finalStatus) => (appId, appAttempt) -> (date, finalStatus)}

    val endDates = exitCodes.join(endStatus).filter{case (_, (code, (_,status))) => status=="FAILED" || code !=0}
        .map{case (key, (code, (date,status))) => key -> date}

    /*
    First i join start dates and end dates.
    Hence: StartJoinEnd looks like ((appID, attempt), (StartDate, EndDate))
    */
    val startJoinEnd = startTimes.join(endDates)

    /*
    Next I join with containers on the AppId. Hence I have: ((AppId, attempt), (Dates, Status), List(containerID, Host))
    To prepare for the join with users, it is necessary to move the attempt number to the values, we will then move it back
    to the key after joining with users.
     */
    val datesJoinContainers = startJoinEnd.join(containers).mapValues {
      case (l, buff) => (l, buff.toList.map(x => (x._3, x._4)))
    }.map {
      case (key,value) =>
        (key._1, (key._2, value))
    }

    /*
    Final RDD: We join with users. AllData looks something like: ((AppId, User, AppAttempt), List(Dates, Status), List(ContainerID, Hosts)), ...)
     */
    val allData = datesJoinContainers.join(appUsers).map {
      case (key, values) => ((key, values._2, values._1._1), values._1._2)
    }.filter(x => x._1._1 >= startId && x._1._1 <= endId)

    allData.collect.foreach(println)
  }
}

package com.satish.workshop.spark.common

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

class DateUtils {
  val DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def getCurrentTimeStamp() : Timestamp = {
    val today = Calendar.getInstance().getTime
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    return Timestamp.valueOf(dateFormat.format(today))
  }

  def getPastTimeStamp() : Timestamp = {
    val cal = Calendar.getInstance()
    val dateStr = "1990-01-01 00:00:00"
    return getTimeStamp(dateStr)
  }

  def getTimeStamp(dateStr : String) : Timestamp = {
    val cal = Calendar.getInstance()
    cal.setTime(DATEFORMAT.parse(dateStr))
    return Timestamp.valueOf(DATEFORMAT.format(cal.getTime))
  }

  def addInterval(dateStr: String, interval: Int, inputFormat: String, outputFormat: String) : String = {
    val dateInputFormat: SimpleDateFormat = new SimpleDateFormat(inputFormat)
    val dateOutputFormat: SimpleDateFormat = new SimpleDateFormat(outputFormat)
    println(dateInputFormat.parse(dateStr))
    val cal = Calendar.getInstance()
    cal.setTime(dateInputFormat.parse(dateStr))
    cal.add(Calendar.HOUR, interval)
    return dateOutputFormat.format(cal.getTime)
  }

  def dateAddDay(date: String, days: Int, inputFormat: String, outputFormat: String) : String = {
    val dateAux = Calendar.getInstance()
    dateAux.setTime(new SimpleDateFormat(inputFormat).parse(date))
    println(dateAux.getTime())
    dateAux.add(Calendar.DATE, days)
    println(dateAux.getTime())
    return new SimpleDateFormat(outputFormat).format(dateAux.getTime())
  }

  /*
    Scala doesn’t have API its own for Dates and timestamps, so we need to depend on Java libraries.
    Here is the quick method to get current datetimestamp and format it as per your required format.
   */
  import java.sql.Timestamp
  def getCurrentdateTimeStamp: Timestamp = {
    val today:java.util.Date = Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now:String = timeFormat.format(today)
    val ts = java.sql.Timestamp.valueOf(now)
    ts
  }
}

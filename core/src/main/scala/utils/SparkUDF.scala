package ir.mci.dwbi.bigdata.spark_job.core.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object SparkUDF {
  val toJalaliUDF: UserDefinedFunction = udf((date: String) => {
    if (date != null && date.nonEmpty) {
      try {
        Some(DateConverter.toJalali(date).toInt)
      } catch {
        case _: NumberFormatException => None
      }
    } else {
      None
    }
  })

  val getMsLocationPGWUDF: UserDefinedFunction = udf((mccmnc: String, lac: String, cell: String) => {
    def str_int_internal(value: String): Int = {
      try {
        if (value == null || value == "") {
          0
        } else if (value.charAt(0) == '-') {
          value.toInt
        } else {
          ('0' + value).toInt
        }
      } catch {
        case _: Throwable => 0
      }
    }

    val zlac = "%05d".format(str_int_internal(lac))
    val zcell = "%05d".format(str_int_internal(cell))

    if (mccmnc == "43211") {
      val mslocation = mccmnc + zlac + zcell
      if (mslocation.length <= 15) {
        mslocation
      } else {
        mccmnc + lac + zcell.substring(0, zcell.length - 3) + zcell.charAt(zcell.length - 1)
      }
    } else {
      "-1"
    }
  })

  val getMsLocationCBSUDF: UserDefinedFunction = udf((c: String) => {
    try {
      if (c.trim.length >= 5) {
        if (c.substring(0, 5) == "43211") {
          if (c.length == 15) {
            c
          }
          else {
            "43211" + Integer.parseInt(c.substring(5, 5 + 4), 16).toString + (Integer.parseInt(c.substring(9), 16) / 256).toString + (Integer.parseInt(c.substring(9), 16) % 256).toString
          }
        }
        else {
          c.substring(0, 5)
        }
      }
      else {
        "-1"
      }
    }
    catch {
      case _: Throwable => "0"
    }
  }
  )

  val getDiffTimeUDF: UserDefinedFunction = udf((beg: String, end: String) => {
    try {
      val f = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
      val t1 = LocalDateTime.parse(DateConverter.toGeo(beg) + beg.substring(8), f)
      val t2 = LocalDateTime.parse(DateConverter.toGeo(end) + end.substring(8), f)
      ChronoUnit.SECONDS.between(t1, t2).toInt
    }
    catch {
      case _: Throwable => 0
    }
  }
  )
}

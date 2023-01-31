package pws.netops.fpp.intervalmon

import java.io.File
import java.text.SimpleDateFormat
import java.util.{ Date, Calendar }
import pws.netops.common.data.Time
import pws.netops.common.log._
import pws.netops.common.system.CondorSubmit.{ mkSubmitDataShared,
  writeSubmitFile, submit }
import pws.netops.common.system.NetopsTailer
import scala.collection.mutable.ListBuffer
import scala.util.{ Failure, Success }

import pws.netops.fpp.Options


object IntervalMonitor {

  val df = new SimpleDateFormat("yyyyMMddHHmm")


  def mkMatcher(opts: Options) = """Timing for Writing wrfout_d%02d_(\d\d\d\d)-(\d\d)-(\d\d)_(\d\d):(\d\d).*""".format(opts.productOpts.domain).r


  def mkVdts(opts: Options): List[String] = {
    val c = Calendar.getInstance
    c.setTimeInMillis(Time.cycleToInitTime(opts.cycle))

    def genHours(idx: Int, tfhs: List[String]): List[String] = {
      if (idx >= opts.intervalOpts.totalIntervals) tfhs
      else {
        val c2 = c.clone.asInstanceOf[Calendar]

        c2.add(Calendar.MINUTE, (idx * opts.intervalOpts.intervalDuration))

        genHours(idx + 1, tfhs ++: List(df.format(
          new Date(c2.getTimeInMillis))))
      }
    }

    genHours(0, Nil)
  }


  def doWork(opts: Options, cmddebug: Boolean = false) = {
    val l = Log.getLogger

    l.info("Interval Monitor started")

    val intervalFilePath = "%s/%s".format(opts.productOpts.cycleDir,
      opts.productOpts.intervalFile)

    if (cmddebug)
      l.info(s"Started in debug mode, not submitting Grib Conversion jobs")

    l.info(s"Looking for file: ${intervalFilePath}")

    val matcher = mkMatcher(opts)

    val intervalFormatter = new SimpleDateFormat("yyyyMMddHHmm z")

    val vdts = ListBuffer.empty ++= mkVdts(opts)

    def procLine(line: String): Unit = {
      line match {
        case matcher(yyyy, mon, dd, hh, min) => {
          val testFH = "%04d%02d%02d%02d%02d".format(
            yyyy.toInt, mon.toInt, dd.toInt, hh.toInt, min.toInt)

          if (vdts.contains(testFH)) {
            l.info(s"Found forecast interval: ${yyyy + mon + dd + hh + min}   ${vdts.length - 1} left")

            if (!cmddebug)
              startGribConversion(opts, df.parse(testFH).getTime)

            vdts.remove(vdts.indexOf(testFH))
          }
        }
        case _ => {}
      }
    }

    val intervalFile = new File(intervalFilePath)

    val nOTailer = new NetopsTailer(intervalFile, procLine)

    val tailer = nOTailer.start(500)

    def wait4tailer(wait: Long): Unit = {
      if (wait > 0 && vdts.size > 0) {
        Thread.sleep(500)
        wait4tailer(wait - 500)
      }
    }

    wait4tailer(opts.intervalOpts.intervalMonTimeout)

    tailer.stop

    if (vdts.size == 0) l.info("Interval Monitor complete")
    else
      l.severe("Interval Monitor failed to find all intervals in time")

  }


  val postDirFmt = new SimpleDateFormat("yyyy'-'MM'-'dd'_'HHmm")

  def startGribConversion(opts: Options, valid_dt: Long) {
    val l = Log.getLogger

    // Use this for each submission
    val formattedVDT = df.format(valid_dt)

    val postWorkingDir = opts.productOpts.postWorkingDir + "/" +
      postDirFmt.format(valid_dt)
      
    (new File(postWorkingDir)).mkdirs

    // Submit each of these to Condor
    val submitPathPrefix =
      s"${postWorkingDir}/condor_grib_conversion_job.${formattedVDT}.sh"

    val submitContents = mkSubmitDataShared(
      s"${opts.productOpts.installFQP}/bin/fpp-gribconv",
      Options.mkPropsArgs(opts) +
        s"-c ${opts.cycle} " +
        s"-v $valid_dt " +
        s"-r ${opts.runId}",
      submitPathPrefix,
      opts.gribOpts.gribConvCondorRAM)

    val result = submitContents flatMap writeSubmitFile flatMap submit

    result match {
      case Success(msg) => l.fine(msg)
      case Failure(ex)  => l.severe(s"Condor submit failed!\n${ex}")
    }
  }

}

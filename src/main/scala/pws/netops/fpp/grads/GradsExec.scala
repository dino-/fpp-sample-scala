package pws.netops.fpp.grads

import java.io.{ File, FileWriter }
import java.text.DecimalFormat
import java.text.SimpleDateFormat
import java.util.{ Date, Calendar, TimeZone }
import pws.netops.common.data.Time
import pws.netops.common.log._
import pws.netops.common.system.Process
import scala.util.{ Failure, Success }

import pws.netops.fpp.Options


class GradsExec(opts: Options, valid_dt: Long, script: String, region: String) {
  val wrfoutFmt = new SimpleDateFormat("yyyy'-'MM'-'dd'_'HH:mm:ss")
  val postDirFmt = new SimpleDateFormat("yyyy'-'MM'-'dd'_'HHmm")
  val vdtNameFmt = new SimpleDateFormat("yyyyMMdd_HHmm")

  // Cycle time in millis
  val cycleMillis = Time.cycleToInitTime(opts.cycle)

  val myFH = (valid_dt - Time.cycleToInitTime(opts.cycle)) / (60 * 1000.0 * 60.0)

  val myIntervalNum = ((valid_dt - cycleMillis) / (opts.intervalOpts.intervalDuration * 60 * 1000)).toInt
  
  val gradsWorkingDir = opts.productOpts.postWorkingDir + "/" + postDirFmt.format(new Date(valid_dt))

  def doWork = {
    val l = Log.getLogger

    val neededFiles = IntervalTaskFactory.genFileList(valid_dt, script, opts.cycle)

    val valid_dtS = vdtNameFmt.format(new Date(valid_dt))

    try {
      // job environment setup
      val cmdString = new StringBuffer()

      cmdString.append("cd %s;".format(gradsWorkingDir))
      cmdString.append("ln -sf %s/include/* .;".format(opts.gradsOpts.gradsTemplatesDir))
      cmdString.append("cp %s/pws/%s %s.%s;".format(opts.gradsOpts.gradsTemplatesDir,
        script,
        script,
        valid_dtS))
      cmdString.append("mkdir -p ../images;")
      cmdString.append("ln -sf ../images .")

      Process.execCmd(cmdString.toString)

      //      val rtGradsFilename = "%s.%03d".format(script, myFH)
      val condorScriptName = buildJob(
        opts.gradsOpts.gradsBaseDir,
        opts.gradsOpts.gradsTemplatesDir + "/pws",
        gradsWorkingDir,
        s"gs.${region}.${script}.${valid_dtS}", myFH)

      new File(gradsWorkingDir + "/" + condorScriptName).setExecutable(true)

      l.fine(s"Created script: ${gradsWorkingDir}/${condorScriptName}")

      val gradsCmd = s"${gradsWorkingDir}/${condorScriptName}"

      def wait4files(needFiles: List[String]): List[String] = {
        def loopEm(myNeedFiles: List[String], time2go: Long): List[String] = {
          if (myNeedFiles.length == 0 || time2go <= 0) {
            myNeedFiles
          } else {
            val waitTime = 30 * 1000
            Thread.sleep(waitTime)
            loopEm(myNeedFiles.filter(f => {
              val fF = new File(f)
              fF.exists
            }), time2go - waitTime)
          }
        }
        loopEm(needFiles, opts.gradsOpts.gradsExecTimeout)
      }

      // need to call wait4files
      neededFiles match {
        case Some(ndf) => {
          // We got a list
          wait4files(ndf) match {
            case x if x.length > 0 => {
              // We didn't find everything we need before the time. Log and bail
              l.severe(s"Could not file some needed control files: ${x}")
            }
            case x if x.length == 0 => {
              // We got everything. Let's go
              Process.execCmd(gradsCmd)
            }
          }
        }
        case None => {
          l.warning(s"We're not far enough into the cycle for this GrADS script: ${script}   Interval: ${myIntervalNum}")
        }
      }
      l.info("Grads job complete")

    } catch {
      case e: Exception => println("GradsWorker[%s] failed on %s. Exception: %s\n%s)".format(valid_dtS, script, e.getMessage, e.getStackTraceString))
    }

  }


  val decOnlyIfNeeded = new DecimalFormat("##.#")

  def buildJob (gradsBaseDir: String, gradsTemplateDirFQP: String,
    fhtRuntimeFQP: String, rtGradsFilename: String,
    fh: Double) : String = {

    val displayFh = decOnlyIfNeeded.format(fh)

    //check loop condition
    //@todo refactor

    var rtGradsFQP = fhtRuntimeFQP + "/" + rtGradsFilename;

    val condorScriptName = "grads_job." + rtGradsFilename + ".sh";
    val fout = new FileWriter(fhtRuntimeFQP + "/" + condorScriptName);
    fout.write("#!/bin/bash\n\n");
    fout.write("exec 1>%s.out 2>%s.err\n\n".format(fhtRuntimeFQP + "/" + condorScriptName, fhtRuntimeFQP + "/" + condorScriptName))
    fout.write("umask 000 \n");
    fout.write("hostname \n");
    fout.write("date\n")
    fout.write("cd " + fhtRuntimeFQP + "\n\n");

    // FIXME Ask somebody (Meredith? Pete?) about this. This directory is totally different from the one in the GADDIR variable in the "stock" environment. What gives?
    //check environment and get rid of this        
    fout.write("export GADDIR=%s/data \n".format(opts.gradsOpts.gradsBaseDir));

    fout.write("FILESIZE=-1\n");
    fout.write("RETRIES=15\n");
    fout.write(s"FILENAME='images/${script.substring(3)}.${myIntervalNum}.png'\n");

    fout.write("while [ $RETRIES -gt 0 -a $FILESIZE -lt 10000 ]; do\n");

    // Load the FOS params and regions to get the grads args
    val (_, regions) = GradsParams.load(s"${opts.productOpts.installFQP}/config/${opts.productOpts.productName}.grparam") match {
      case Failure(e) => throw e
      case Success(t) => t
    }
    val (_, gradsArgs) = regions.get(region)

    val locCycleDateHr = getFHLocalDT("America/New_York", Time.cycleToInitTime(opts.cycle))
    val locValidDateHr = getFHLocalDT("America/New_York", valid_dt)
    val ctlPrime = "%s_d%02d.%s.grib.ctl".format(
         opts.productOpts.productName.toLowerCase, opts.productOpts.domain,
         vdtNameFmt.format(new Date(valid_dt)))
    val ctlOthers = "INITIAL.ctl MINUS1.ctl MINUS2.ctl MINUS3.ctl MINUS4.ctl MINUS5.ctl MINUS6.ctl MINUS7.ctl"

    fout.write(s"""${opts.gradsOpts.gradsExec} -blcx "run ${gradsTemplateDirFQP}/${script} ${gradsArgs} ${displayFh} ${myIntervalNum} ${locCycleDateHr} ${locValidDateHr} ${region} ${ctlPrime} ${ctlOthers}" > ${rtGradsFilename}.log\n""")

    fout.write("if [ -f $FILENAME ]; then\n")
    fout.write("FILESIZE=$(stat -c%s \"$FILENAME\")\n")
    fout.write("fi\n")

    fout.write("let RETRIES-=1\n")
    fout.write("sleep 1\n")
    fout.write("done\n")

    fout.write("RETVAL=$?")

    fout.write("\ndate\n")

    fout.write("""exit $RETVAL""")

    fout.flush()
    fout.close()

    condorScriptName
  }


  // These date/times need to be sent to grads as two parameters
  // each. This function busts our validDT into these parts with
  // a space.
  def getFHLocalDT(tz: String, validDT: Long): String = {
    val formatter = new SimpleDateFormat("MM/dd/yyyy HH")

    val validDTCal = Calendar.getInstance
    validDTCal.setTimeInMillis(validDT)
    validDTCal.setTimeZone(TimeZone.getTimeZone(tz))
    formatter.format(validDTCal.getTime())
  }

}

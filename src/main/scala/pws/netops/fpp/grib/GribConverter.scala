package pws.netops.fpp.grib

import java.io.{ File, FileWriter }
import java.text.SimpleDateFormat
import java.util.Date
import pws.netops.common.data.Time
import pws.netops.common.data.OptionUtil.{ optionOr }
import pws.netops.common.log._
import pws.netops.common.system.IO.{ writeFile }
import scala.io.Source.{ fromFile }
import scala.language.postfixOps
import scala.sys.process._

import pws.netops.fpp.Options


/*
 *  GribConvertor class
 *  This will take the raw netcdf as generated by the model and
 *  generate a grib2 files
 *
 *    Step One: unipost
 *      requirements:
 *        netcdf file - generated by model
 *        itag file - contains parameters for unipost command.
 *          Generated below
 *        wrf_cntl.parm - parameter file from WRF. symlink to master copy
 *        fort.14 - symlink copy of wrf_cntl.parm
 */
object GribConverter {

  val wrfoutFmt = new SimpleDateFormat("yyyy'-'MM'-'dd'_'HH:mm:ss")
  val postDirFmt = new SimpleDateFormat("yyyy'-'MM'-'dd'_'HHmm")
  val vdtNameFmt = new SimpleDateFormat("yyyyMMdd_HHmm")
  val tdefDateFmt = new SimpleDateFormat("HH'Z'ddMMMyyyy")

  def getPostWorkingDir(optWorkingDir: String, valid_dt: Long): String = {
    s"${optWorkingDir}/${postDirFmt.format(new Date(valid_dt))}"
  }


  // do deps. big ugly shell command that creates the interval
  // working dir and symlinking
  def createWorkingDirStructure(opts: Options, postWorkingDir: String) = {
    (new File(postWorkingDir)).mkdirs

    val shareDir = s"${opts.productOpts.installFQP}/share"
    val cmd =
      """cd %s;
ln -sf %s/ETAMPNEW_DATA eta_micro_lookup.dat;
ln -sf %s/wrf_cntrl.parm .;
ln -sf ./wrf_cntrl.parm fort.14""".format(
        postWorkingDir,
        shareDir,
        shareDir)

    Seq("/bin/bash", "-c", cmd).run
  }

  // function to create UPP based grib gen job
  def doWork(opts: Options, valid_dt: Long): Option[String] = {
    val l = Log.getLogger

    val dateVdt = new Date(valid_dt)

    val vdtName = vdtNameFmt.format(dateVdt)

    val myFH = Time.getFHFromValidDT(valid_dt,
      Time.cycleToInitTime(opts.cycle))

    // do setup for wrfpost dependencies
    // working unipost grib filename formatting @local
    val gribFHStr = "%03d".format(myFH - 3)
    val gribFilePrefix = s"${opts.productOpts.productName.toLowerCase}_d0${opts.productOpts.domain}.${vdtNameFmt.format(new Date(valid_dt))}"
    val gribFileName = s"${gribFilePrefix}.grib"
    val ctlFileName = s"${gribFilePrefix}.grib.ctl"
    val idxFileName = s"${gribFilePrefix}.grib.idx"
    val tdefDate = tdefDateFmt.format(new Date(valid_dt))

    // Cycle time in millis
    val cycleMillis = Time.cycleToInitTime(opts.cycle)

    val myIntervalNum = ((valid_dt - cycleMillis) / (opts.intervalOpts.intervalDuration * 60 * 1000)).toInt

    val wrfoutWithColons = wrfoutFmt.format(dateVdt)

    val postWorkingDir = getPostWorkingDir(opts.productOpts.postWorkingDir, valid_dt)

    // compose itag file for unipost
    val netcdfFQP = opts.productOpts.cycleDir + "/wrfout_d0" +
      opts.productOpts.domain + "_" + wrfoutWithColons

    if (!netcdfIsGood(netcdfFQP, myFH)) l.warning(
      s"UPPJob:NetCDF file check failed for ${netcdfFQP} ... proceeding anyway. Wish me luck!")

    createWorkingDirStructure(opts, postWorkingDir)

    val homeDir = System.getProperty("user.home")

    // Create the ctl file
    val ctlTemplate =
      fromFile(s"${opts.productOpts.installFQP}/share/${opts.productOpts.productName.toLowerCase}.ctl.template").mkString
    val ctlContents = ctlTemplate.format(gribFileName, idxFileName,
      gribFileName, tdefDate)
    writeFile(s"${postWorkingDir}/${ctlFileName}", ctlContents)

    // Now the itag file
    val fout = new FileWriter(postWorkingDir + "/itag")
    fout.write(netcdfFQP + "\n");
    fout.write("netcdf\n")
    fout.write(wrfoutWithColons + "\n")
    fout.write("NCAR\n")
    fout.flush()
    fout.close()

    // Create the grib/idx file script
    val gribScriptName = s"grib_job.${vdtName}.sh"
    val gribScriptContents = new StringBuilder("#!/bin/bash\n")
    gribScriptContents.append("cd $(dirname $0)\n")
    gribScriptContents.append(
      s"exec 1>${gribScriptName}.out 2>${gribScriptName}.err\n")
    gribScriptContents.append("hostname\n")
    gribScriptContents.append(
      s"export PATH=$$PATH:${opts.gradsOpts.gradsBaseDir}/bin;\n")

    // use unipost to convert netcdf to grib
    gribScriptContents.append(s"stat ${netcdfFQP}\n")
    gribScriptContents.append("sleep 15\n")
    gribScriptContents.append(s"stat ${netcdfFQP}\n")
    
    gribScriptContents.append("RETRY=0\n")
    gribScriptContents.append("DOIT=1\n")
    gribScriptContents.append("while [ $RETRY -le 5 -a $DOIT -ne 0 ]; do\n")
    gribScriptContents.append("sleep 5\n")
    gribScriptContents.append("let RETRY+=1\n")
    gribScriptContents.append(s"${homeDir}/bin/unipost.exe < itag > unipost_d0${opts.productOpts.domain}.${vdtName}.out\n")
    gribScriptContents.append(s"mv WRFPRS.Grb* ${gribFileName}\n")
    gribScriptContents.append("DOIT=$?\n")
    gribScriptContents.append("done\n")

    val idxCmd = s"${homeDir}/bin/gribmap -i ${ctlFileName}\n"
    gribScriptContents.append(idxCmd)

    // write grib creation script
    val gribScriptPath = s"${postWorkingDir}/${gribScriptName}"
    val gribScriptFile = new File(gribScriptPath)
    val gribScriptWriter = new FileWriter(gribScriptFile)
    gribScriptWriter.write(gribScriptContents.toString)
    gribScriptWriter.flush
    gribScriptWriter.close
    gribScriptFile.setExecutable(true)

    // exec the script
    val ec = gribScriptPath !

    l.info(s"${gribScriptName} grib conversion completed with exit code ${ec}")

    // We need to link our grib, ctl, and idx files to all interval directories
    if (valid_dt == cycleMillis) {
      Range(1, opts.intervalOpts.totalIntervals).foreach(interval => {
        // Figure out the milli for vdt
        val myVDT = cycleMillis + (interval * opts.intervalOpts.intervalDuration * 60l * 1000l)
        // get postWorkingDir for interval
        val nextVDDir = getPostWorkingDir(opts.productOpts.postWorkingDir, myVDT)

        // Now build the cmd
        val cmd = new StringBuffer()
        // Create the directory
        cmd.append("mkdir -p %s;".format(nextVDDir))
        // link the ctl file
        cmd.append("ln -fs %s/%s %s/INITIAL.ctl;".format(postWorkingDir, ctlFileName, nextVDDir))
        // link the grib file
        cmd.append("ln -fs %s/%s %s/%s;".format(postWorkingDir, gribFileName, nextVDDir, gribFileName))
        // link the idx file
        cmd.append("ln -fs %s/%s %s/%s;".format(postWorkingDir, idxFileName, nextVDDir, idxFileName))

        // Run it
        Seq("/bin/bash", "-c", cmd.toString).run
      })
    }

    // Create the links for the MINUS files
    def minusLinks(intervals: Int) = {
      // Find the number of hours we can link
      def getHours(hours:Int):Int = {
        val lastVDT = cycleMillis + (opts.intervalOpts.intervalDuration * (opts.intervalOpts.totalIntervals - 1) * 60 * 1000)
        
        def loopIt(hoursLeft:Int):Int = {
          val curVDT = valid_dt + (hoursLeft * 60 * 60 * 1000)
          if (hoursLeft > hours ||
              curVDT >= lastVDT) {
            hoursLeft
          } else {
            loopIt(hoursLeft + 1)
          }
        }
        loopIt(0)
      }
      
      // Make sure we don't go over the total intervals
      val numHourLinks = getHours(intervals)
      
      Range(1, numHourLinks + 1).foreach(cur => {
        // Figure out the milli for vdt
        val myVDT = valid_dt + (cur * 60 * 60 * 1000)
        // get postWorkingDir for interval
        val nextVDDir = getPostWorkingDir(opts.productOpts.postWorkingDir, myVDT)

        l.fine("Creating links for MINUS%d".format(cur))
        // Now build the cmd
        val cmd = new StringBuffer()
        // Create the directory
        cmd.append("mkdir -p %s;".format(nextVDDir))
        // link the ctl file
        cmd.append("ln -fs %s/%s %s/MINUS%d.ctl;".format(postWorkingDir, ctlFileName, nextVDDir, cur))
        // link the grib file
        cmd.append("ln -fs %s/%s %s/%s;".format(postWorkingDir, gribFileName, nextVDDir, gribFileName))
        // link the idx file
        cmd.append("ln -fs %s/%s %s/%s;".format(postWorkingDir, idxFileName, nextVDDir, idxFileName))

        // Run it
        Seq("/bin/bash", "-c", cmd.toString).run

      })
    }
    minusLinks(6)

    fileCompletionStatus(postWorkingDir, gribFileName, ctlFileName, idxFileName, opts)
  }

  // We need to check the sizes of the three files produced by the
  // grib conversion

  def fileCompletionStatus(
    postWorkingDir: String,
    gribFileName: String,
    ctlFileName: String,
    idxFileName: String,
    opts: Options): Option[String] = {

    // This function checks that a file is bigger than the given size.
    // It returns None if it's fine, and Some(errorMsg) if it's not,
    // so we can report what exactly happened here.
    def checkFile(t: (String, Long)): Option[String] = {
      val (fileName, minSize) = t
      val fqp = s"${postWorkingDir}/${fileName}"
      val f = new File(fqp)
      if (f.length > minSize) None
      else Some(s"PROBLEM: file is too small: ${fqp}")
    }

    // Map the checkFile function above over a list of pairs of
    // filename, size values. This gives us a list of Option[String]
    // results.
    val statuses = List(
      (gribFileName, opts.gribOpts.gribFileMinSize),
      (ctlFileName, 3072L), // 3k
      (idxFileName, 10240L)) // 10k
      .map(checkFile)

    // Report back the first "failure" (non-None value). This is
    // extracted from the list using a fold.
    statuses.foldLeft(None: Option[String])(optionOr)
  }

  // check to make sure NetCDF file has completed writing
  def netcdfIsGood(fqp: String, myFH: Int): Boolean = {
    val l = Log.getLogger

    val minSize = 2000000000L

    def log(addl: String) {
      l.finer(s"GribConverter[${myFH}].netcdfIsGood():${fqp} ${addl}")
    }

    def wait = Thread.sleep(5 * 1000)

    def isGood(remainingTries: Int, oldSize: Option[Long]): Boolean =
      if (remainingTries < 0) false
      else {
        val curSize = try {
          val fp = new java.io.File(fqp)
          if (fp.length < minSize) {
            log(s"filesize: ${fp.length}")
            wait
          }
          Some(fp.length)
        } catch {
          case e: Exception => {
            log(s"exception: ${e.getMessage}")
            wait
            None
          }
        }

        log(s"-- ${curSize} = ${oldSize} ?")

        wait
        if (curSize == oldSize) true
        else isGood(remainingTries - 1, curSize)
      }

    isGood(30, Some(0L))
  }

}

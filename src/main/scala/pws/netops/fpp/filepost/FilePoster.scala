package pws.netops.fpp.filepost

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Date
import pws.netops.common.data.Time
import pws.netops.common.log._
import pws.netops.common.system.Process
import scala.util.{ Failure, Success }

import pws.netops.fpp.grads.GradsParams
import pws.netops.fpp.Options


class FilePoster(opts: Options, valid_dt: Long, script: String, region: String) {

  val wrfoutFmt = new SimpleDateFormat("yyyy'-'MM'-'dd'_'HH:mm:ss")
  val postDirFmt = new SimpleDateFormat("yyyy'-'MM'-'dd'_'HHmm")
  val vdtNameFmt = new SimpleDateFormat("yyyyMMdd_HHmm")

  val valid_dtDate = new Date(valid_dt)

  val gradsWorkingDir = opts.productOpts.postWorkingDir + "/" + postDirFmt.format(valid_dtDate)

  val myFH = (valid_dt - Time.cycleToInitTime(opts.cycle)) / (60 * 1000.0 * 60.0)

  val intervalNum = (valid_dt - Time.cycleToInitTime(opts.cycle)) / (opts.intervalOpts.intervalDuration * 60 * 1000)

  val imgName = s"${region}.${script}.${intervalNum}.png"

  val imagePath = "%s/images/%s".format(gradsWorkingDir, imgName)


  def doWork = {
    val l = Log.getLogger

    val valid_dtS = vdtNameFmt.format(new Date(valid_dt))

    // Load the FOS params and regions
    val (params, regions) = GradsParams.load(s"${opts.productOpts.installFQP}/config/${opts.productOpts.productName}.grparam") match {
      case Failure(e) => throw e
      case Success(t) => t
    }

    // Then update FOS with REST call
    val postBody = addFile(opts.runId.toInt,
      Time.cycleToInitTime(opts.cycle),
      valid_dt,
      opts.productOpts.priority,
      imgName,
      opts.publishOpts.publishDir.substring(opts.publishOpts.publishDir.indexOf("/", 1) + 1), // RoR kluge to strip of beginning of FOS NFS path
      (params get script),
      (regions get region)._1,
      intervalNum.toInt,
      opts.intervalOpts.intervalDuration,
      opts.publishOpts.FOSRestURL)

    val scriptNameFQP = s"${gradsWorkingDir}/filepost.${region}.${script}.${valid_dtS}.sh"

    l.info(s"Creating filepost script) ${scriptNameFQP}")

    val fout = new FileWriter(scriptNameFQP)

    fout.write("""#!/bin/bash

exec 1>%s.out 2>%s.err
""".format(scriptNameFQP, scriptNameFQP))

    fout.write("""
umask 000
cd %s
hostname""".format(gradsWorkingDir))

/* old script code
# FIXME Do the while retries thing here
if [ -f %s ]; then
fileSize=$(du -bs %s|cut -f 1)
else
  echo "Image file is missing"
  exit 4
fi
*/
    fout.write("""          
## do publishing steps

FILESIZE=-1
RETRIES=5
TARGETSIZE=5000
while [ $RETRIES -gt 0 -a $FILESIZE -lt $TARGETSIZE ]; do
   if [ -f %s ]; then
      FILESIZE=$(du -bs %s|cut -f 1)
   fi
   let RETRIES-=1
   sleep 1
done

echo "Graphic filesize: $FILESIZE"

if [ ${FILESIZE} > ${TARGETSIZE} ];then
  echo "File OK...posting"
  mkdir -p %s
  cp %s %s
  chmod 666 %s/%s
  sleep 1
  /usr/bin/curl -d "%s" %s
else
  echo "Image file was too small"
  exit 5
fi
""".format(
      imagePath, imagePath, opts.publishOpts.publishDir, imagePath, 
      opts.publishOpts.publishDir, opts.publishOpts.publishDir,
      imgName, postBody, opts.publishOpts.FOSRestURL))
    fout.write("\n")

    fout.close

    Process.execCmd("/bin/bash " + scriptNameFQP)
    
    l.info("FilePost completed")
  }


  def addFile(runId: Int,
    initTime: Long,
    validDT: Long,
    priority: Int,
    fileName: String,
    fileDir: String,
    paramId: Int,
    regionId: Int,
    interval: Int,
    intervalDuration: Int,
    restURL: String) = {
    val body = "<insert><file><init_time>" + initTime + "</init_time>" +
      "<valid_datetime>" + validDT + "</valid_datetime><priority>" + priority + "</priority>" +
      "<filename>" + fileName + "</filename><filepath>" + fileDir + "</filepath>" +
      "<run_id>" + runId + "</run_id><parameter_id>" + paramId + "</parameter_id><region_id>" + regionId + "</region_id>" +
      "<filetype_id>1</filetype_id><intervalduration>" + intervalDuration + "</intervalduration>" +
      "<intervalnumber>" + interval + "</intervalnumber><storm_id>0</storm_id></file></insert>"
    body
  }

}

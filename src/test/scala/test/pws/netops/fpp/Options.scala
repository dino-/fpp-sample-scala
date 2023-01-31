package test.pws.netops.fpp

import java.io.File
import org.scalatest.FunSuite
import pws.netops.fpp.{Options => O}
 

class Options extends FunSuite {

   test ("load properties") {
      val gradsDir = new File("/tmp/pws")
      gradsDir.mkdir
      val cycle = "2015052100"
      val opts = O.load(cycle, List("src/test/resources/test.props"))
      gradsDir.delete

      //println(opts)
      assert(opts.productOpts.product === 8)
      assert(opts.productOpts.installFQP ===
         "/lustre/ucar_home/trop3d1/RegionalConus")
      assert(opts.productOpts.postWorkingDir ===
         s"/lustre/ucar_home/trop3d1/nwprod/tropics/prod/${cycle}/postproc_development")
      assert(opts.intervalOpts.totalIntervals === 121)
      assert(opts.gribOpts.gribFileMinSize === 209715200L)
      assert(opts.gradsOpts.gradsTemplatesDir ===
         "/lustre/ucar_home/trop3d1/grads/tropical_atl/development")
      assert(opts.publishOpts.publish === false)
      assert(opts.publishOpts.publishDir === s"/output_files/tropical_atl/${cycle}/1")
      assert(opts.runId === 0L)
   }

}

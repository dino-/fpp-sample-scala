package test.pws.netops.fpp.grads

import org.scalatest.FunSuite
import pws.netops.fpp.grads.{GradsParams => G}
 

class GradsParams extends FunSuite {

   test ("load bad grads param file") {
      val actual = G.load("foo/bar")

      assert(actual.isFailure)
   }


   test ("load good grads param file") {
      val actual = G.load("src/test/resources/test.grparam")

      val expectedParams = Map(
         "STtpcpnIn" -> 5,
         "RhPct700WndKt" -> 120,
         "Hgt925AvorWndKt" -> 114,
         "Pcpn3InMslpWndKt" -> 124,
         "Pcpn6InMslpWndKt" -> 125,
         "Pcpn1InMslpWndKt" -> 123,
         "SWindKtMslp" -> 72,
         "SReflectDbz" -> 38,
         "VshearKt850to250" -> 77,
         "SGstKtWndKt" -> 109,
         "HgtM500Mslp" -> 17)

      val expectedRegions = Map(
         "TROP" -> (1, "3 48 -110 -22 17 1 1 10.3 3.8 1 0.7 9.5 0.7"),
         "WATL" -> (6, "15 47 -91 -49 10 1 1 10.1 3.8 1 0.1 9.5 0.1"),
         "ATL" -> (2, "10 44 -82 -23 10 1 1 10.3 3.8 1 0.3 9.5 0.3"),
         "GCAR" -> (3, "14 33 -100 -67 8 1 1 10.3 3.8 1 0.30 9.5 0.3"),
         "EPAC" -> (12, "10 37 -133 -89 10 1 1 10.3 3.8 1 0.1 9.5 0.1"))

      assert(actual.isSuccess)
      val (actualParams, actualRegions) = actual.get
      assert(actualParams.v === expectedParams)
      assert(actualRegions.v === expectedRegions)
   }


   test ("get a param id") {
      val tryResult = G.load("src/test/resources/test.grparam")
      assert(tryResult.isSuccess)
      val (params, _) = tryResult.get
      assert ((params get "RhPct700WndKt") === 120)
   }


   test ("get a region id") {
      val tryResult = G.load("src/test/resources/test.grparam")
      assert(tryResult.isSuccess)
      val (_, regions) = tryResult.get
      assert ((regions get "GCAR")._1 === 3)
   }

}

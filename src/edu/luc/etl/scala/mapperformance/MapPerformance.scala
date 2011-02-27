package edu.luc.etl.scala.mapperformance

import scala.util.Random

object MapPerformance {

  def main(args: Array[String]): Unit = {
	val Array(n, d, k1, k2) = args map (_.toInt)
	println(n, d, k1, k2)
	checkArgs(n, d, k1, k2)
	new MapPerformance(n, d, k1, k2).run
  }

  def checkArgs(n: Int, d: Int, k1: Int, k2: Int) = {
	assert(0 <= d && d <= 100, "invalid percentage d")
	assert(2 * n <= Math.pow(Byte.MaxValue - Byte.MinValue, k2 - k1), "insufficient key range")
  }
}

class MapPerformance(val n: Int, val d: Int, val k1: Int, val k2: Int) {
	
  def run = {
	val t0 = System.currentTimeMillis
	val map1 = takeTime("creating")(growMap(nextKey)(n)(Map.empty))
	val map2 = takeTime("shrinking")(shrinkMap(d)(map1))
	val map3 = takeTime("regrowing")(growMap(nextKey)(n)(map2))
	val t3 = System.currentTimeMillis
	println((t3 - t0).toFloat / 1000 + "s total")
  }
  
  def takeTime[T](msg: String)(task: => Map[T, T]) = {
	println(msg)
	val t0 = System.currentTimeMillis
	val m = task
	val t1 = System.currentTimeMillis
	println(m.size + " elements")
	println((t1 - t0).toFloat / 1000 + "s")
	m
  }
  
  def nextKey = {
	val length = k1 + Random.nextInt(k2 - k1)
	val sb = new StringBuilder(length)
	for (i <- 0 until length)
	  sb.append(Random.nextPrintableChar)
	sb.toString
  }

  def growMap[T](gen: => T)(n: Int)(m: Map[T, T]): Map[T, T] =
	Stream.continually(gen).scanLeft(m)((m: Map[T, T], k: T) => m + ((k, k))).find((m: Map[T, T]) => m.size >= n).get
  
  def shrinkMap[T](perc: Int)(m: Map[T, T]) = {
	val n1 = perc * m.size / 100
	(Random shuffle (m.keys take n1)).foldLeft(m)(_ - _) 
  }
}

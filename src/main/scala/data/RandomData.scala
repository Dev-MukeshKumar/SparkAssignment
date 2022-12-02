package data

import scala.annotation.tailrec
import scala.util.Random

object RandomData {

  val alpha = "abcdefghijklmnopqrstuvwxyz"

  @tailrec
  def randomName(n: Int, acc: String = ""): String = {
    if (n <= 1) acc
    else randomName(n - 1, acc + "" + alpha.charAt(Random.nextInt(26)))
  }

  def yesOrNo(): Boolean = Random.nextInt(2) match {
    case 0 => false
    case 1 => true
    case _ => false
  }

  def randomScore(): Int = 50 + Random.nextInt(101)
}

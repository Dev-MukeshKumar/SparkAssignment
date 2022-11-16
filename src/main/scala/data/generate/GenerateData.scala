package data.generate

import scala.annotation.tailrec
import scala.util.Random

object GenerateData {

  val alpha="abcdefghijklmnopqrstuvwxyz"

  @tailrec
  def getData(n:Int,count:Int=0,data:List[RecordSchema]=List[RecordSchema](),bankId:Int=100,accountId:Int=1,stateCode:Int=1): List[RecordSchema] ={
    if(count>=n) data
    else if (stateCode == 10) getData(n,count+1,data :+ RecordSchema(stateCode,bankId,randomName(6),accountId,randomScore(),yesOrNo()),bankId+1,accountId+1)
    else getData(n,count+1,data :+ RecordSchema(stateCode,bankId,randomName(6),accountId,randomScore(),yesOrNo()),bankId+1,accountId+1,stateCode+1)
  }

  @tailrec
  def randomName(n: Int, acc: String = ""): String = {
    if (n <= 1) acc
    else randomName(n - 1, acc + "" + alpha.charAt(Random.nextInt(26)))
  }

  def yesOrNo():Boolean = Random.nextInt(2) match {
    case 0 => false
    case 1 => true
    case _ => false
  }

  def randomScore(): Int = 50 + Random.nextInt(101)
}

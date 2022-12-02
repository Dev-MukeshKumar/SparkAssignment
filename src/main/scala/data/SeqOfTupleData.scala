package data

import scala.annotation.tailrec
import data.RandomData._

object SeqOfTupleData {
  @tailrec
  def generateData(n:Int,count:Int=0,data:Seq[(Int,Int,String,Int,Int,Boolean)]=Seq[(Int,Int,String,Int,Int,Boolean)](),bankId:Int=100,accountId:Int=1,stateCode:Int=1): Seq[(Int,Int,String,Int,Int,Boolean)] ={
    if(count>=n) data
    else if (stateCode == 10) generateData(n,count+1,data :+ (stateCode,bankId,randomName(6),accountId,randomScore(),yesOrNo()),bankId+1,accountId+1)
    else generateData(n,count+1,data :+ (stateCode,bankId,randomName(6),accountId,randomScore(),yesOrNo()),bankId+1,accountId+1,stateCode+1)
  }
}

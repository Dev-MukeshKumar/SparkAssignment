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

/*
  columns list
    1. bank id (id type - unique for each record)
    2. area name (string type)
    3. state code (id type - common for 100 dataset)
    4. account id (id type - unique for each record)
    5. account credit score (int type)
    6. credit card (boolean type)
*/
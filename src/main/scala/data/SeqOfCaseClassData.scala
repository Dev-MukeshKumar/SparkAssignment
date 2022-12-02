package data

import data.RandomData._

import scala.annotation.tailrec

object SeqOfCaseClassData {
  @tailrec
  def generateData(n: Int, count: Int = 0, data: Seq[RecordCaseClassSchema] = Seq[RecordCaseClassSchema](), bankId: Int = 100, accountId: Int = 1, stateCode: Int = 1): Seq[RecordCaseClassSchema] = {
    if (count >= n) data
    else if (stateCode == 10) generateData(n, count + 1, data :+ RecordCaseClassSchema(stateCode, bankId, randomName(6), accountId, randomScore(), yesOrNo()), bankId + 1, accountId + 1)
    else generateData(n, count + 1, data :+ RecordCaseClassSchema(stateCode, bankId, randomName(6), accountId, randomScore(), yesOrNo()), bankId + 1, accountId + 1, stateCode + 1)
  }
}

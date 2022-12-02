package data

import data.RandomData._

import scala.annotation.tailrec

object SeqOfClassData {
  @tailrec
  def generateData(n: Int, count: Int = 0, data: Seq[RecordClassSchema] = Seq[RecordClassSchema](), bankId: Int = 100, accountId: Int = 1, stateCode: Int = 1): Seq[RecordClassSchema] = {
    if (count >= n) data
    else if (stateCode == 10) generateData(n, count + 1, data :+ new RecordClassSchema(stateCode, bankId, randomName(6), accountId, randomScore(), yesOrNo()), bankId + 1, accountId + 1)
    else generateData(n, count + 1, data :+ new RecordClassSchema(stateCode, bankId, randomName(6), accountId, randomScore(), yesOrNo()), bankId + 1, accountId + 1, stateCode + 1)
  }
}

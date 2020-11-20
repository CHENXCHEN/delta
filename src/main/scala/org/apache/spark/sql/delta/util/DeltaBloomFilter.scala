/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.util

import java.io._
import java.nio.charset.StandardCharsets
import java.util.UUID

import javax.xml.bind.DatatypeConverter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.bloom.{BloomFilter, Key}
import org.apache.hadoop.util.hash.Hash

import org.apache.spark.Partitioner
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.commands.MergeIntoCommand
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

case class BFItem(fileName: String, bf: String, size: Int, sizePretty: String)

/**
 * A Bloom filter implementation built on top of {@link org.apache.hadoop.util.bloom.BloomFilter}.
 */
object DeltaBloomFilter extends Logging {
  /**
   * Used in computing the optimal Bloom filter size. This approximately equals 0.480453.
   */
  val LOG2_SQUARED: Double = Math.log(2) * Math.log(2)

  def apply(numEntries: Int, errorRate: Double): DeltaBloomFilter =
    DeltaBloomFilter(numEntries, errorRate, Hash.MURMUR_HASH)

  def apply(numEntries: Int, errorRate: Double, hashType: Int): DeltaBloomFilter = {
    // Bit size
    val bitSize = Math.ceil(numEntries * (-Math.log(errorRate) / LOG2_SQUARED)).toInt
    // Number of the hash functions
    val numHashs = Math.ceil(Math.log(2) * bitSize / numEntries).toInt
    // The filter
    new DeltaBloomFilter(
      new BloomFilter(bitSize, numHashs, hashType)
    )
  }

  def apply(filterStr: String): DeltaBloomFilter = {
    val bloomFilter = new BloomFilter
    val bytes = DatatypeConverter.parseBase64Binary(filterStr)
    val dis = new DataInputStream(new ByteArrayInputStream(bytes))
    try {
      bloomFilter.readFields(dis)
      dis.close()
    } catch {
      case e: IOException =>
        throw new RuntimeException("Could not deserialize BloomFilter instance", e)
    }
    new DeltaBloomFilter(
      bloomFilter
    )
  }

  def getBloomFilterPath(version: Long): String = {
    val runId = UUID.randomUUID().toString
    FileNames.bloomFilterChildPrefix(version) + runId
  }

  def addBloomFilterMeta(deltaTxn: OptimisticTransaction): Unit = {
    deltaTxn.updateMetadata(
      deltaTxn.metadata.copy(
        configuration = deltaTxn.metadata.configuration
            + (DeltaConfigs.BLOOM_FILTER_LOCATION.key ->
            getBloomFilterPath(deltaTxn.snapshot.version + 1))
      )
    )
  }

  def getNameIndexFromSchema(schema: StructType): Map[String, Int] = {
    schema.zipWithIndex.map { case (it, index) =>
      it.name -> index
    }.toMap
  }

  def makeBFKey(idCols: Seq[String], row: Row, name2Index: Map[String, Int]): String = {
    idCols.map { idName =>
      row.get(name2Index(idName)).toString
    }.mkString("_")
  }

  def getRelativeChildPath(dataPath: String, absolutePath: String): String = {
    absolutePath.split(dataPath).last.stripPrefix("/")
  }

  def generateBloomFilterFile(
      spark: SparkSession
      , deltaLog: DeltaLog
      , deltaTxn: OptimisticTransaction
      , deltaActions: Seq[FileAction]
      , sourceSchema: StructType): Unit = {
    import spark.implicits._
    val idCols = getDeltaIDCols(deltaLog)
    val bFErrorRate = DeltaConfigs.BLOOM_FILTER_ERROR_RATE.fromMetaData(deltaLog.snapshot.metadata)

    val addFiles = deltaActions.filter(_.isInstanceOf[AddFile]).map(_.asInstanceOf[AddFile])
    val deletedFiles = deltaActions.filter(_.isInstanceOf[RemoveFile])
        .map(_.asInstanceOf[RemoveFile])
    val deletePaths = deletedFiles.map(f => f.path).toSet
    val newBFLocation = DeltaConfigs.BLOOM_FILTER_LOCATION.fromMetaData(deltaTxn.metadata)
    val newBFPathFs = new Path(deltaTxn.deltaLog.dataPath, newBFLocation)

    val oldBFLocationOpt = Option(DeltaConfigs.BLOOM_FILTER_LOCATION
        .fromMetaData(deltaLog.snapshot.metadata))
        .filter(_.nonEmpty)

    val shouldGenerateBFAddFiles: Seq[AddFile] =
      if (oldBFLocationOpt.nonEmpty) {
        val oldBFPathFs = new Path(deltaLog.dataPath, oldBFLocationOpt.get)
        deltaTxn.deltaLog.fs.mkdirs(newBFPathFs)
        spark.read.parquet(oldBFPathFs.toUri.getPath).repartition(1).as[BFItem].
            filter { f =>
              !deletePaths.contains(f.fileName)
            }.write.mode(SaveMode.Overwrite).parquet(newBFPathFs.toUri.getPath)
        addFiles
      } else {
        // if old bloom filter path do not exist, we should generate all files except deleted file
        val allFiles = deltaLog.snapshot.allFiles.collect()
        allFiles.filter(x => !deletePaths.contains(x.path)).toSeq ++ addFiles
      }
    logInfo(s"should generate bloom filter files: $shouldGenerateBFAddFiles")

    val FILE_NAME = MergeIntoCommand.FILE_NAME_COL

    val df = deltaLog.createDataFrame(deltaLog.snapshot, shouldGenerateBFAddFiles
      , false, sourceSchema = sourceSchema)
        .select(idCols.map(col): _*)
        .withColumn(FILE_NAME, input_file_name)

    val dataPath = deltaLog.dataPath.toUri.getPath
    val name2Index = DeltaBloomFilter.getNameIndexFromSchema(df.schema)
    val id2AddFiles = shouldGenerateBFAddFiles.zipWithIndex.map {
      case (file, i) => file.path -> i
    }.toMap
    logInfo(s"generate bloom filter, get add files: ${shouldGenerateBFAddFiles.length}")
    df.rdd.mapPartitions(rows => {
      rows.map {
        row =>
          (row.getAs[String](FILE_NAME), makeBFKey(idCols, row, name2Index))
      }
    })
        .partitionBy(new Partitioner {
          override def numPartitions: Int = shouldGenerateBFAddFiles.length

          override def getPartition(key: Any): Int =
            id2AddFiles(getRelativeChildPath(dataPath, key.toString))
        })
        .mapPartitions {
          it: Iterator[(String, String)] =>
            var fileName: String = null
            val buffer = it.map {
              case (fName, kk) =>
                if (fileName == null) fileName = fName
                kk
            }.toArray
            if (buffer.nonEmpty) {
              val bf = DeltaBloomFilter(buffer.length, bFErrorRate)
              buffer.foreach(x => bf.add(x))
              List(BFItem(
                getRelativeChildPath(dataPath, fileName),
                bf.serializeToString, bf.size, (bf.size / 8d / 1024 / 1024) + "m"
              )).iterator
            } else List.empty[BFItem].iterator
        }
        .coalesce(1).toDF().as[BFItem].write.mode(SaveMode.Append)
        .parquet(newBFPathFs.toUri.getPath)
  }


  /** check if bloom filter version match with snapshot */
  def checkBloomFilterIfExist(deltaLog: DeltaLog): Boolean = {
    val bfLocation = DeltaConfigs.BLOOM_FILTER_LOCATION.fromMetaData(deltaLog.snapshot.metadata)
    if (bfLocation.isEmpty) false
    else {
      val bfPath = new Path(bfLocation)
      try {
        FileNames.isBloomFilterFile(bfPath) &&
          bfLocation.split(FileNames.bloomFilterDirPrefix)
            .last.split("_").head.toLong == deltaLog.snapshot.version
      } catch {
        case e: Throwable =>
          logError(e.getMessage, e)
          false
      }
    }
  }

  def conf: SQLConf = SQLConf.get

  def addBFConfiguration(configuration: Map[String, String]
      , deltaTxn: DeltaLog): Map[String, String] = {

    var alterConfiguration = configuration
    alterConfiguration += (
        DeltaConfigs.BLOOM_FILTER_LOCATION.key ->
            DeltaBloomFilter.getBloomFilterPath(deltaTxn.snapshot.version + 1)
        )
    if (!alterConfiguration.contains(DeltaConfigs.ID_COLS.key)) {
      val idCols = getDeltaIDCols(deltaTxn)
      assert(idCols.nonEmpty)
      alterConfiguration += (
          DeltaConfigs.ID_COLS.key -> idCols.sorted.mkString(",")
          )
    }
    alterConfiguration
  }

  def getDeltaIDCols(deltaLog: DeltaLog): Seq[String] = {
    Option(DeltaConfigs.ID_COLS.fromMetaData(deltaLog.snapshot.metadata))
        .filter(_.nonEmpty).orElse(conf.getConf(DeltaSQLConf.DELTA_ID_COLS)
        .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
    ).get
  }
}

class DeltaBloomFilter(filter: org.apache.hadoop.util.bloom.BloomFilter) {

  def size: Int = this.filter.getVectorSize

  def add(key: String): Unit = {
    assert(key != null, "Key cannot by null")
    filter.add(new Key(key.getBytes(StandardCharsets.UTF_8)))
  }

  def mightContain(key: String): Boolean = {
    assert(key != null, "Key cannot by null")
    filter.membershipTest(new Key(key.getBytes(StandardCharsets.UTF_8)))
  }

  /**
   * Serialize the bloom filter as a string.
   */
  def serializeToString: String = {
    val baos = new ByteArrayOutputStream
    val dos = new DataOutputStream(baos)
    try {
      filter.write(dos)
      val bytes = baos.toByteArray
      dos.close()
      DatatypeConverter.printBase64Binary(bytes)
    } catch {
      case e: IOException =>
        throw new RuntimeException("Could not serialize BloomFilter instance", e)
    }
  }
}

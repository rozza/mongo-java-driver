/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala

import com.mongodb.ClusterFixture.getServerApi
import com.mongodb.connection.ServerVersion
import org.mongodb.scala.bson.BsonString
import org.scalatest._

import scala.collection.JavaConverters._
import scala.concurrent.duration.{ Duration, _ }
import scala.concurrent.{ Await, ExecutionContext }
import scala.util.{ Properties, Try }

trait RequiresMongoDBISpec extends BaseSpec with BeforeAndAfterAll {

  private object GlobalState {
    val mongoClientURI: String = {
      val uri = Properties.propOrElse(MONGODB_URI_SYSTEM_PROPERTY_NAME, DEFAULT_URI)
      if (!uri.isBlank) uri else DEFAULT_URI
    }
    val connectionString: ConnectionString = ConnectionString(mongoClientURI)

    def mongoClientSettingsBuilder: MongoClientSettings.Builder = {
      val builder = MongoClientSettings.builder().applyConnectionString(connectionString)
      if (getServerApi != null) {
        builder.serverApi(getServerApi)
      }
      builder
    }

    val mongoClientSettings: MongoClientSettings = mongoClientSettingsBuilder.build()
    val mongoClient: MongoClient = MongoClient(mongoClientSettings)
  }

  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  private val DEFAULT_URI: String = "mongodb://localhost:27017/"
  private val MONGODB_URI_SYSTEM_PROPERTY_NAME: String = "org.mongodb.test.uri"
  val WAIT_DURATION: Duration = 60.seconds
  private val DB_PREFIX = "mongo-scala-"
  private var _currentTestName: Option[String] = None
  private var mongoDBOnline: Boolean = false

  protected override def runTest(testName: String, args: Args): Status = {
    _currentTestName = Some(testName.split("should")(1))
    mongoDBOnline = isMongoDBOnline()
    super.runTest(testName, args)
  }

  /**
   * The database name to use for this test
   */
  def databaseName: String = DB_PREFIX + suiteName

  /**
   * The collection name to use for this test
   */
  def collectionName: String = _currentTestName.getOrElse(suiteName).filter(_.isLetterOrDigit)

  val mongoClientURI: String = GlobalState.mongoClientURI
  val connectionString: ConnectionString = GlobalState.connectionString

  def mongoClientSettingsBuilder: MongoClientSettings.Builder = GlobalState.mongoClientSettingsBuilder

  val mongoClientSettings: MongoClientSettings = GlobalState.mongoClientSettings

  def mongoClient(): MongoClient = GlobalState.mongoClient

  def isMongoDBOnline(): Boolean = {
    Try(Await.result(GlobalState.mongoClient.listDatabaseNames().toFuture(), WAIT_DURATION)).isSuccess
  }

  def hasSingleHost(): Boolean = {
    GlobalState.connectionString.getHosts.size() == 1
  }

  def checkMongoDB() {
    if (!mongoDBOnline) {
      cancel("No Available Database")
    }
  }

  def withClient(testCode: MongoClient => Any): Unit = {
    checkMongoDB()
    testCode(GlobalState.mongoClient) // loan the client
  }

  def withDatabase(dbName: String)(testCode: MongoDatabase => Any) {
    withClient { client =>
      val databaseName = if (dbName.startsWith(DB_PREFIX)) dbName.take(63) else s"$DB_PREFIX$dbName".take(63) // scalastyle:ignore
      val mongoDatabase = client.getDatabase(databaseName)
      try testCode(mongoDatabase) // "loan" the fixture to the test
      finally {
        // clean up the fixture
        Await.result(mongoDatabase.drop().toFuture(), WAIT_DURATION)
      }
    }
  }

  def withDatabase(testCode: MongoDatabase => Any): Unit = withDatabase(databaseName)(testCode: MongoDatabase => Any)

  def withCollection(testCode: MongoCollection[Document] => Any) {
    withDatabase(databaseName) { mongoDatabase =>
      val mongoCollection = mongoDatabase.getCollection(collectionName)
      try testCode(mongoCollection) // "loan" the fixture to the test
      finally {
        // clean up the fixture
        Await.result(mongoCollection.drop().toFuture(), WAIT_DURATION)
      }
    }
  }

  lazy val isSharded: Boolean = if (!mongoDBOnline) {
    false
  } else {
    Await
      .result(
        GlobalState.mongoClient.getDatabase("admin").runCommand(Document("isMaster" -> 1)).toFuture(),
        WAIT_DURATION
      )
      .getOrElse("msg", BsonString(""))
      .asString()
      .getValue == "isdbgrid"
  }

  lazy val buildInfo: Document = {
    if (mongoDBOnline) {
      Await.result(
        GlobalState.mongoClient.getDatabase("admin").runCommand(Document("buildInfo" -> 1)).toFuture(),
        WAIT_DURATION
      )
    } else {
      Document()
    }
  }

  def serverVersionAtLeast(minServerVersion: List[Int]): Boolean = {
    buildInfo.get[BsonString]("version") match {
      case Some(version) =>
        val serverVersion = version.getValue.split("\\D+").map(_.toInt).padTo(3, 0).take(3).toList.asJava
        new ServerVersion(serverVersion.asInstanceOf[java.util.List[Integer]])
          .compareTo(new ServerVersion(minServerVersion.asJava.asInstanceOf[java.util.List[Integer]])) >= 0
      case None => false
    }
  }

  def serverVersionLessThan(maxServerVersion: List[Int]): Boolean = {
    buildInfo.get[BsonString]("version") match {
      case Some(version) =>
        val serverVersion = version.getValue.split("\\D+").map(_.toInt).padTo(3, 0).take(3).toList.asJava
        new ServerVersion(serverVersion.asInstanceOf[java.util.List[Integer]])
          .compareTo(new ServerVersion(maxServerVersion.asJava.asInstanceOf[java.util.List[Integer]])) < 0
      case None => false
    }
  }

  override def beforeAll() {
    if (mongoDBOnline) {
      Await.result(GlobalState.mongoClient.getDatabase(databaseName).drop().toFuture(), WAIT_DURATION)
    }
  }

  override def afterAll() {
    if (mongoDBOnline) {
      Await.result(GlobalState.mongoClient.getDatabase(databaseName).drop().toFuture(), WAIT_DURATION)
    }
  }

  Runtime.getRuntime.addShutdownHook(new ShutdownHook())

  private[mongodb] class ShutdownHook extends Thread {
    override def run() {
      GlobalState.mongoClient.getDatabase(databaseName).drop()
      GlobalState.mongoClient.close()
    }
  }

}

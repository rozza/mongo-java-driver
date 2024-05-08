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

import org.reflections.Reflections
import org.reflections.scanners.Scanners.SubTypes
import org.reflections.util.ConfigurationBuilder
import org.scalatest.Inspectors.forEvery

import java.lang.reflect.Modifier
import java.lang.reflect.Modifier.*
import scala.collection.JavaConverters.*

class ApiAliasAndCompanionSpec extends BaseSpec {

  private val defaultClassFilter = (f: Class[_ <: Object]) => {
    isPublic(f.getModifiers) && (!isAbstract(f.getModifiers) || isInterface(f.getModifiers)) &&
    f.getSimpleName != "package" && f.getSimpleName != "package$" && !f.getSimpleName.equals("Builder") &&
    (f.getCanonicalName == null || (
      !f.getCanonicalName.contains("Spec") &&
        !f.getCanonicalName.contains("Test") &&
        !f.getCanonicalName.contains("Tour") &&
        !f.getCanonicalName.contains("Fixture")
    ))
  }

  private val scalaMapper = (name: String) => {
    val removedDollar = if (name.endsWith("$")) name.dropRight(1) else name
    removedDollar.replace("Publisher", "Observable")
  }

  "The scala package" should "mirror the com.mongodb package and com.mongodb.reactivestreams.client" in {
    val javaExclusions = Set(
      "Address",
      "AggregatePrimer",
      "AuthenticationMechanism",
      "AwsCredential",
      "BSONTimestampCodec",
      "BasicDBList",
      "BasicDBObject",
      "BasicDBObjectBuilder",
      "Block",
      "CausalConsistencyExamples",
      "ChangeStreamSamples",
      "ContextHelper",
      "ContextProvider",
      "DBObject",
      "DBObjectCodec",
      "DBObjectCodecProvider",
      "DBRef",
      "DBRefCodec",
      "DBRefCodecProvider",
      "DnsClient",
      "DnsClientProvider",
      "DocumentToDBRefTransformer",
      "DocumentationSamples",
      "ErrorCategory",
      "ExplainVerbosity",
      "Function",
      "FutureResultCallback",
      "IndexesPrimer",
      "InetAddressResolver",
      "InetAddressResolverProvider",
      "InsertPrimer",
      "Jep395RecordCodecProvider",
      "KerberosSubjectProvider",
      "KotlinCodecProvider",
      "MongoClients",
      "NonNull",
      "NonNullApi",
      "Nullable",
      "Person",
      "PublisherHelpers",
      "QueryPrimer",
      "ReactiveContextProvider",
      "ReadConcernLevel",
      "ReadPreferenceHedgeOptions",
      "RemovePrimer",
      "RequestContext",
      "ServerApi",
      "ServerApiVersion",
      "ServerCursor",
      "ServerSession",
      "SessionContext",
      "SingleResultCallback",
      "SubjectProvider",
      "SubscriberHelpers",
      "SyncClientEncryption",
      "SyncGridFSBucket",
      "SyncMongoClient",
      "SyncMongoDatabase",
      "TargetDocument",
      "TransactionExample",
      "UnixServerAddress",
      "UpdatePrimer",
      "WriteError"
    )

    val javaPackageExclusions = List(
      "com.mongodb.annotations",
      "com.mongodb.assertions",
      "com.mongodb.async",
      "com.mongodb.binding",
      "com.mongodb.bulk",
      "com.mongodb.connection",
      "com.mongodb.crypt",
      "com.mongodb.diagnostics",
      "com.mongodb.event",
      "com.mongodb.internal",
      "com.mongodb.kotlin",
      "com.mongodb.management",
      "com.mongodb.operation",
      "com.mongodb.reactivestreams",
      "com.mongodb.selector",
      "com.mongodb.test"
    )

    val javaFilter: Class[? <: Object] => Boolean = cls => {
      if (defaultClassFilter.apply(cls)) {
        // Exclude the above and any nested static classes
        if (javaExclusions.contains(cls.getSimpleName) || cls.getName.contains("$")) false
        else cls.getCanonicalName != null && !javaPackageExclusions.exists(cls.getCanonicalName.startsWith(_))
      } else false
    }

    val publishers = reflectPackage(
      "com.mongodb.reactivestreams.client",
      cls => cls.getSimpleName.endsWith("Publisher")
    )
    val wrapped = reflectPackage("com.mongodb", javaFilter) ++ publishers

    val scalaExclusions = Set(
      "CreateIndexCommitQuorum",
      "Document",
      "Helpers",
      "MongoClient",
      "MongoCollection",
      "MongoDatabase",
      "MongoException",
      "Observable",
      "Observer",
      "ReadConcernLevel",
      "ReadPreference",
      "SingleObservable",
      "Subscription"
    )

    val implicitsFilter: Class[? <: Object] => Boolean = cls =>
      defaultClassFilter.apply(cls) && cls.getCanonicalName != null && cls.getCanonicalName.contains("Implicits")

    val scalaImplicits = reflectPackage("org.mongodb.scala", implicitsFilter)
    val local = reflectPackage(
      "org.mongodb.scala",
      cls => defaultClassFilter.apply(cls) && !scalaExclusions.contains(cls.getSimpleName.replace("$", ""))
    ) -- scalaImplicits

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror parts of com.mongodb.connection in org.mongodb.scala.connection" in {
    val javaExclusions = Set(
      "AsyncCompletionHandler",
      "ClusterDescription",
      "ClusterId",
      "ClusterConnectionMode",
      "ClusterType",
      "ConnectionDescription",
      "ConnectionId",
      "ServerDescription",
      "ServerId",
      "ServerVersion",
      "TopologyVersion",
      "ServerMonitoringMode",
      "ServerConnectionState"
    )

    val wrapped = reflectPackage("com.mongodb.connection") -- javaExclusions
    val local = reflectPackage("org.mongodb.scala.connection") - "TransportSettings"

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror all com.mongodb.client. into org.mongodb.scala." in {
    val packageName = "com.mongodb.client"

    val javaExclusions = Set(
      "ClientSession",
      "ConcreteCodecProvider",
      "FailPoint",
      "Fixture",
      "ImmutableDocument",
      "ImmutableDocumentCodec",
      "ImmutableDocumentCodecProvider",
      "ListCollectionsObservable",
      "MongoChangeStreamCursor",
      "MongoClientFactory",
      "MongoClients",
      "MongoCursor",
      "MongoObservable",
      "Name",
      "NameCodecProvider",
      "SynchronousContextProvider",
      "TransactionBody",
      "WithWrapper"
    )

    val wrapped = new Reflections(packageName)
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(_.getPackage.getName == packageName)
      .filter(defaultClassFilter)
      .map(_.getSimpleName.replace("Iterable", "Observable"))
      .toSet -- javaExclusions

    val scalaPackageName = "org.mongodb.scala"
    val local = new Reflections(scalaPackageName)
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(_.getPackage.getName == scalaPackageName)
      .filter((f: Class[_ <: Object]) => isPublic(f.getModifiers))
      .map(_.getSimpleName.replace("$", ""))
      .toSet

    forEvery(wrapped) { (className: String) =>
      local should contain(className)
    }
  }

  it should "mirror all com.mongodb.client.model in org.mongodb.scala.model" in {
    val scalaAliases = Set(
      "ApproximateQuantileMethod",
      "CreateIndexOptions",
      "DropIndexOptions",
      "GeoNearOptions",
      "TimeSeriesGranularity",
      "Window",
      "WindowOutputField"
    )

    val scalaImplicits = Set("ScalaOptionDoubleToJavaDoubleOrNull")
    val wrapped = reflectPackage("com.mongodb.client.model")
    val local = (reflectPackage("org.mongodb.scala.model") ++ scalaAliases) -- scalaImplicits

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror all com.mongodb.client.model.search in org.mongodb.scala.model.search" in {
    val scalaAliases = Set(
      "AddSearchScoreExpression",
      "AutocompleteSearchOperator",
      "CompoundSearchOperator",
      "CompoundSearchOperatorBase",
      "ConstantSearchScore",
      "ConstantSearchScoreExpression",
      "DateNearSearchOperator",
      "DateRangeSearchOperator",
      "DateRangeSearchOperatorBase",
      "DateSearchFacet",
      "ExistsSearchOperator",
      "FacetSearchCollector",
      "FieldSearchPath",
      "FilterCompoundSearchOperator",
      "FunctionSearchScore",
      "GaussSearchScoreExpression",
      "GeoNearSearchOperator",
      "Log1pSearchScoreExpression",
      "LogSearchScoreExpression",
      "LowerBoundSearchCount",
      "MultiplySearchScoreExpression",
      "MustCompoundSearchOperator",
      "MustNotCompoundSearchOperator",
      "NumberNearSearchOperator",
      "NumberRangeSearchOperator",
      "NumberRangeSearchOperatorBase",
      "NumberSearchFacet",
      "PathBoostSearchScore",
      "PathSearchScoreExpression",
      "RelevanceSearchScoreExpression",
      "ShouldCompoundSearchOperator",
      "StringSearchFacet",
      "TextSearchOperator",
      "TotalSearchCount",
      "ValueBoostSearchScore",
      "WildcardSearchPath"
    )

    val wrapped = reflectPackage("com.mongodb.client.model.search")
    val local = reflectPackage("org.mongodb.scala.model.search") ++ scalaAliases

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror all com.mongodb.client.model.geojson in org.mongodb.scala.model.geojson" in {
    val wrapped = reflectPackage("com.mongodb.client.model.geojson")
    val local = reflectPackage("org.mongodb.scala.model.geojson")

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror all com.mongodb.client.result in org.mongodb.scala.result" in {
    val wrapped = reflectPackage("com.mongodb.client.result")
    val local = reflectPackage("org.mongodb.scala.result")

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror all com.mongodb.reactivestreams.client.vault in org.mongodb.scala.vault" in {
    val wrapped = reflectPackage("com.mongodb.reactivestreams.client.vault")
    val local = reflectPackage("org.mongodb.scala.vault")

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror all com.mongodb.WriteConcern in org.mongodb.scala.WriteConcern" in {
    val notMirrored = Set(
      "SAFE",
      "serialVersionUID",
      "FSYNCED",
      "FSYNC_SAFE",
      "JOURNAL_SAFE",
      "REPLICAS_SAFE",
      "REPLICA_ACKNOWLEDGED",
      "NAMED_CONCERNS",
      "NORMAL",
      "majorityWriteConcern",
      "valueOf"
    )
    val wrapped =
      (classOf[com.mongodb.WriteConcern].getDeclaredMethods ++ classOf[com.mongodb.WriteConcern].getDeclaredFields)
        .filter(f => isStatic(f.getModifiers) && !notMirrored.contains(f.getName))
        .map(_.getName)
        .toSet

    val local = WriteConcern.getClass.getDeclaredMethods
      .filter(f => f.getName != "apply" && isPublic(f.getModifiers))
      .map(_.getName)
      .toSet

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror com.mongodb.reactivestreams.client.gridfs in org.mongodb.scala.gridfs" in {
    val wrapped = reflectPackage("com.mongodb.reactivestreams.client.gridfs") - "GridFSBuckets"
    val local = reflectPackage("org.mongodb.scala.gridfs") - "ToGridFSUploadObservableUnit"

    diff(local, wrapped) shouldBe empty
  }

  def diff(a: Set[String], b: Set[String]): Set[String] = a.diff(b) ++ b.diff(a)

  def reflectPackage(
      packageName: String,
      classFilter: Class[? <: Object] => Boolean = defaultClassFilter
  ): Set[String] = {
    val reflections = new Reflections(packageName, SubTypes.filterResultsBy(_ => true))
    val objectsAndEnums =
      reflections.getSubTypesOf(classOf[Object]).asScala ++
        reflections.getSubTypesOf(classOf[Enum[_]]).asScala

    objectsAndEnums
      .filter(_.getPackage.getName == packageName)
      .filter(classFilter)
      .map(_.getSimpleName)
      .filter(!_.isBlank)
      .map(scalaMapper)
      .toSet
  }

  def reflectWithBuilder(
      configurationBuilder: ConfigurationBuilder,
      classFilter: Class[? <: Object] => Boolean = defaultClassFilter
  ): Set[String] = reflect(new Reflections(configurationBuilder), classFilter)

  def reflect(reflections: Reflections, classFilter: Class[? <: Object] => Boolean): Set[String] = {
    val objectsAndEnums =
      reflections.getSubTypesOf(classOf[Object]).asScala ++
        reflections.getSubTypesOf(classOf[Enum[_]]).asScala

    objectsAndEnums
      .filter(classFilter)
      .map(_.getSimpleName)
      .filter(!_.isBlank)
      .map(scalaMapper)
      .toSet
  }

}

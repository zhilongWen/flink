/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.nodes.physical.common

import org.apache.flink.table.catalog.{CatalogBaseTable, CatalogTable}
import org.apache.flink.table.connector.source.ScanTableSource
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.utils.RelExplainUtil

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.hint.RelHint

import java.util

import scala.collection.JavaConverters._

/** Base physical RelNode to read data from an external source defined by a [[ScanTableSource]]. */
abstract class CommonPhysicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: util.List[RelHint],
    relOptTable: TableSourceTable)
  extends TableScan(cluster, traitSet, hints, relOptTable) {

  protected val tableSourceTable: TableSourceTable = relOptTable.unwrap(classOf[TableSourceTable])

  protected[flink] val tableSource: ScanTableSource =
    tableSourceTable.tableSource.asInstanceOf[ScanTableSource]

  override def deriveRowType(): RelDataType = {
    // TableScan row type should always keep same with its
    // interval RelOptTable's row type.
    relOptTable.getRowType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val version = extractSnapshotVersion()
    super
      .explainTerms(pw)
      .item("fields", getRowType.getFieldNames.asScala.mkString(", "))
      .itemIf("hints", hintsDigest, !getHints.isEmpty)
      .itemIf("version", version.getOrElse(""), version.isDefined)
  }

  def copy(relOptTable: TableSourceTable): RelNode

  def hintsDigest: String = {
    RelExplainUtil.hintsToString(getHints)
  }

  def extractSnapshotVersion(): Option[String] = {
    val originTable: CatalogBaseTable =
      relOptTable.contextResolvedTable.getTable.asInstanceOf[CatalogBaseTable]
    originTable match {
      case catalogTable: CatalogTable if catalogTable.getSnapshot.isPresent =>
        Option(catalogTable.getSnapshot.get().toString)
      case _ => Option.empty
    }
  }
}

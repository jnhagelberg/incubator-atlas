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

package org.apache.atlas.query

import javax.script.{ Bindings, ScriptEngine, ScriptEngineManager }
import org.apache.atlas.repository.graphdb.AtlasGraph
import org.apache.atlas.query.TypeUtils.ResultWithPathStruct
import org.apache.atlas.typesystem.json._
import org.apache.atlas.typesystem.types._
import org.json4s._
import org.json4s.native.Serialization._
import scala.language.existentials
import org.apache.atlas.repository.graphdb.AtlasVertex
import java.util.Collections

case class GremlinQueryResult(query: String,
        resultDataType: IDataType[_],
        rows: List[_]) {
    def toJson = JsonHelper.toJson(this)
}

class GremlinEvaluator[V, E](qry: GremlinQuery, persistenceStrategy: GraphPersistenceStrategies, g: AtlasGraph[V, E]) {

    /**
     *
     * @param gResultObj is the object returned from gremlin. This must be a List
     * @param qryResultObj is the object constructed for the output w/o the Path.
     * @return a ResultWithPathStruct
     */
    def addPathStruct(gResultObj: AnyRef, qryResultObj: Any): Any = {
        if (!qry.isPathExpresion) {
            qryResultObj
        } else {
            import scala.collection.JavaConversions._
            import scala.collection.JavaConverters._
            val pathList = g.convertPathQueryResultToList(gResultObj);
            val iPaths = pathList.asInstanceOf[java.util.List[AnyRef]].init

            val oPaths = iPaths.map { p =>
                var atlasValue = g.convertGremlinValue(p);
                persistenceStrategy.constructInstance(TypeSystem.getInstance().getIdType.getStructType, atlasValue)
            }.toList.asJava
            val sType = qry.expr.dataType.asInstanceOf[StructType]
            val sInstance = sType.createInstance()
            sInstance.set(ResultWithPathStruct.pathAttrName, oPaths)
            sInstance.set(ResultWithPathStruct.resultAttrName, qryResultObj)
            sInstance
        }
    }

    def instanceObject(v: AnyRef): AnyRef = {
        if (qry.isPathExpresion) {
            import scala.collection.JavaConversions._
            var pathAsList = g.convertPathQueryResultToList(v)
            pathAsList.asInstanceOf[java.util.List[AnyRef]].last
        } else {
            v
        }
    }

    def evaluate(): GremlinQueryResult = {
        import scala.collection.JavaConversions._
        val rType = qry.expr.dataType
        val oType = if (qry.isPathExpresion) qry.expr.children(0).dataType else rType
        val rawRes = g.executeGremlinScript(qry.queryStr);

        if (!qry.hasSelectList) {
            val rows = rawRes.asInstanceOf[java.util.List[AnyRef]].map { v =>
                val iV = instanceObject(v)
                val atlasValue = g.convertGremlinValue(iV)
                val o = persistenceStrategy.constructInstance(oType, atlasValue)
                addPathStruct(v, o)
            }
            GremlinQueryResult(qry.expr.toString, rType, rows.toList)
        } else {
            val sType = oType.asInstanceOf[StructType]
            val rows = rawRes.asInstanceOf[java.util.List[AnyRef]].map { r =>
                val rV = instanceObject(r)
                val sInstance = sType.createInstance()
                val selObj = SelectExpressionHelper.extractSelectExpression(qry.expr)
                if (selObj.isDefined)
                {
                    val selExpr = selObj.get.asInstanceOf[Expressions.SelectExpression]
                    selExpr.selectListWithAlias.foreach { aE =>
                    	val cName = aE.alias
                    	val (src, idx) = qry.resultMaping(cName)
                    	val v = g.getGremlinColumnValue(rV, src, idx)
                    	sInstance.set(cName, persistenceStrategy.constructInstance(aE.dataType, v))
                    }
                }
                addPathStruct(r, sInstance)
            }
            GremlinQueryResult(qry.expr.toString, rType, rows.toList)
        }

    }
}

object JsonHelper {

    class GremlinQueryResultSerializer()
            extends Serializer[GremlinQueryResult] {
        def deserialize(implicit format: Formats) = {
            throw new UnsupportedOperationException("Deserialization of GremlinQueryResult not supported")
        }

        def serialize(implicit f: Formats) = {
            case GremlinQueryResult(query, rT, rows) =>
                JObject(JField("query", JString(query)),
                    JField("dataType", TypesSerialization.toJsonValue(rT)(f)),
                    JField("rows", Extraction.decompose(rows)(f)))
        }
    }

    implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
        new TypedReferenceableInstanceSerializer + new BigDecimalSerializer + new BigIntegerSerializer +
        new GremlinQueryResultSerializer

    def toJson(r: GremlinQueryResult): String = {
        writePretty(r)
    }
}

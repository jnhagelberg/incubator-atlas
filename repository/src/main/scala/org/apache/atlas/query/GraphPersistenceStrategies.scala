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

import java.util
import java.util.Date
import scala.collection.JavaConversions._
import org.apache.atlas.query.Expressions.ComparisonExpression
import org.apache.atlas.query.Expressions.ExpressionException
import org.apache.atlas.query.TypeUtils.FieldInfo
import org.apache.atlas.repository.graph.GraphHelper
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection
import org.apache.atlas.repository.graphdb.AtlasVertex
import org.apache.atlas.repository.graphdb.GremlinVersion
import org.apache.atlas.typesystem.ITypedInstance
import org.apache.atlas.typesystem.ITypedReferenceableInstance
import org.apache.atlas.typesystem.persistence.Id
import org.apache.atlas.typesystem.types._
import org.apache.atlas.typesystem.types.DataTypes._
import org.apache.atlas.repository.graphdb.GremlinVersion

/**
 * Represents the Bridge between the QueryProcessor and the Graph Persistence scheme used.
 * Some of the behaviors captured are:
 * - how is type and id information stored in the Vertex that represents an [[ITypedReferenceableInstance]]
 * - how are edges representing trait and attribute relationships labeled.
 * - how are attribute names mapped to Property Keys in Vertices.
 *
 * This is a work in progress.
 */
trait GraphPersistenceStrategies {   
    
    def getSupportedGremlinVersion() : GremlinVersion
    
    /**
     * Name of attribute used to store typeName in vertex
     */
    def typeAttributeName: String

    /**
     * Name of attribute used to store super type names in vertex.
     */
    def superTypeAttributeName: String

    /**
     * Name of attribute used to store guid in vertex
     */
    def idAttributeName : String

    /**
     * Given a dataType and a reference attribute, how is edge labeled
     */
    def edgeLabel(iDataType: IDataType[_], aInfo: AttributeInfo): String

    def traitLabel(cls: IDataType[_], traitName: String): String

    def instanceToTraitEdgeDirection : String = "out"
    def traitToInstanceEdgeDirection = instanceToTraitEdgeDirection match {
      case "out" => "in"
      case "in" => "out"
      case x => x
    }

    /**
     * The propertyKey used to store the attribute in a Graph Vertex.
     * @param dataType
     * @param aInfo
     * @return
     */
    def fieldNameInVertex(dataType: IDataType[_], aInfo: AttributeInfo): String

    /**
     * from a vertex for an [[ITypedReferenceableInstance]] get the traits that it has.
     * @param v
     * @return
     */
    def traitNames[V,E](v: AtlasVertex[V,E]): java.util.List[String]

    def edgeLabel(fInfo: FieldInfo): String = fInfo match {
        case FieldInfo(dataType, aInfo, null, null) => edgeLabel(dataType, aInfo)
        case FieldInfo(dataType, aInfo, reverseDataType, null) => edgeLabel(reverseDataType, aInfo)
        case FieldInfo(dataType, null, null, traitName) => traitLabel(dataType, traitName)
    }

    def fieldPrefixInSelect(): String = {
        
        if(getSupportedGremlinVersion() == GremlinVersion.THREE) {
            //this logic is needed to remove extra results from
            //what is emitted by repeat loops.  Technically 
            //for queries that don't have a loop in them we could just use "it"
            //the reason for this is that in repeat loops with an alias,
            //although the alias gets set to the right value, for some
            //reason the select actually includes all vertices that were traversed
            //through in the loop.  In these cases, we only want the last vertex
            //traversed in the loop to be selected.  The logic here handles that
            //case by converting the result to a list and just selecting the
            //last item from it.
            "((it as Vertex[]) as List<Vertex>).last()"
        }
        else {
            "it"
        }
        
    }

    /**
     * extract the Id from a Vertex.
     * @param dataTypeNm the dataType of the instance that the given vertex represents
     * @param v
     * @return
     */
    def getIdFromVertex[V,E](dataTypeNm: String, v: AtlasVertex[V,E]): Id

    def constructInstance[U,V,E](dataType: IDataType[U], v: java.lang.Object): U

    def gremlinCompOp(op: ComparisonExpression) = {
         if( getSupportedGremlinVersion() == GremlinVersion.TWO) {
             gremlin2CompOp(op);             
         }
         else {
            gremlin3CompOp(op);
         }
     }
    
    private def gremlin2CompOp(op: ComparisonExpression) = op.symbol match {
        case "=" => "T.eq"
        case "!=" => "T.neq"
        case ">" => "T.gt"
        case ">=" => "T.gte"
        case "<" => "T.lt"
        case "<=" => "T.lte"
        case _ => throw new ExpressionException(op, "Comparison operator not supported in Gremlin")
    }
    
    private def gremlin3CompOp(op: ComparisonExpression) = op.symbol match {
        case "=" => "eq"
        case "!=" => "neq"
        case ">" => "gt"
        case ">=" => "gte"
        case "<" => "lt"
        case "<=" => "lte"
        case _ => throw new ExpressionException(op, "Comparison operator not supported in Gremlin")
    }

    def loopObjectExpression(dataType: IDataType[_]) = {
      _typeTestExpression(dataType.getName, "it.object")
    }

    def addGraphVertexPrefix(preStatements : Traversable[String]) = !collectTypeInstancesIntoVar

    /**
     * Controls behavior of how instances of a Type are discovered.
     * - query is generated in a way that indexes are exercised using a local set variable across multiple lookups
     * - query is generated using an 'or' expression.
     *
     * '''This is a very bad idea: controlling query execution behavior via query generation.''' But our current
     * knowledge of seems to indicate we have no choice. See
     * [[https://groups.google.com/forum/#!topic/gremlin-users/n1oV86yr4yU discussion in Gremlin group]].
     * Also this seems a fragile solution, dependend on the memory requirements of the Set variable.
     * For now enabling via the '''collectTypeInstancesIntoVar''' behavior setting. Reverting back would require
     * setting this to false.
     *
     * Long term have to get to the bottom of Gremlin:
     * - there doesn't seem to be way to see the physical query plan. Maybe we should directly interface with Titan.
     * - At least from querying perspective a columnar db maybe a better route. Daniel Abadi did some good work
     *   on showing how to use a columnar store as a Graph Db.
     *
     *
     * @return
     */
    def collectTypeInstancesIntoVar = true

    def typeTestExpression(typeName : String, intSeq : IntSequence) : Seq[String] = {
        if (collectTypeInstancesIntoVar)
            typeTestExpressionMultiStep(typeName, intSeq)
        else
            typeTestExpressionUsingFilter(typeName)
    }

    private def typeTestExpressionUsingFilter(typeName : String) : Seq[String] = {
      Seq(s"""filter${_typeTestExpression(typeName, "it")}""")
    }

    /**
     * type test expression that ends up in the emit clause in
     * loop/repeat steps and a few other places
     */
    private def _typeTestExpression(typeName: String, itRef: String): String = {
        
        if( getSupportedGremlinVersion() == GremlinVersion.TWO) {
             s"""{(${itRef}.'${typeAttributeName}' == '${typeName}') |
       | (${itRef}.'${superTypeAttributeName}' ?
       | ${itRef}.'${superTypeAttributeName}'.contains('${typeName}') : false)}""".
          stripMargin.replace(System.getProperty("line.separator"), "")
        }
        else {
            //gremlin 3
            s"""has('${typeAttributeName}',eq('${typeName}')).or().has('${superTypeAttributeName}',eq('${typeName}'))"""
        }
  }    

    private def propertyValueSet(vertexRef : String, attrName: String) : String = {
        s"""org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils.set(${vertexRef}.values('${attrName})"""        
    }
    
    private def typeTestExpressionMultiStep(typeName : String, intSeq : IntSequence) : Seq[String] = {

        val varName = s"_var_${intSeq.next}"
        Seq(
            newSetVar(varName),
            fillVarWithTypeInstances(typeName, varName),
            fillVarWithSubTypeInstances(typeName, varName),
            if(getSupportedGremlinVersion() == GremlinVersion.TWO) {
                s"$varName._()"
            }
            else {
                //this bit of groovy magic converts the set of vertices in varName into
                //a String containing the ids of all the vertices.  This becomes the argument
                //to g.V().  This is needed because Gremlin 3 does not support
                // _()
                //s"g.V(${varName}.collect{it.id()} as String[])"
                s"g.V(${varName} as Object[])"
            }                
        )
    }

    private def newSetVar(varName : String) = s"def $varName = [] as Set"

    private def fillVarWithTypeInstances(typeName : String, fillVar : String) = {
        s"""g.V().has("${typeAttributeName}", "${typeName}").fill($fillVar)"""
    }

    private def fillVarWithSubTypeInstances(typeName : String, fillVar : String) = {
        s"""g.V().has("${superTypeAttributeName}", "${typeName}").fill($fillVar)"""
    }
}

abstract class GraphPersistenceStrategy1 extends GraphPersistenceStrategies {        
    
    val typeAttributeName = "typeName"
    val superTypeAttributeName = "superTypeNames"
    val idAttributeName = "guid"

    def edgeLabel(dataType: IDataType[_], aInfo: AttributeInfo) = s"__${dataType.getName}.${aInfo.name}"

    def edgeLabel(propertyName: String) = s"__${propertyName}"   

    def traitLabel(cls: IDataType[_], traitName: String) = s"${cls.getName}.$traitName"

    def fieldNameInVertex(dataType: IDataType[_], aInfo: AttributeInfo) = GraphHelper.getQualifiedFieldName(dataType, aInfo.name)

    def getIdFromVertex[V,E](dataTypeNm: String, v: AtlasVertex[V,E]): Id =
        new Id(v.getId.toString, 0, dataTypeNm)

    def traitNames[V,E](v: AtlasVertex[V,E]): java.util.List[String] = {
        val s = v.getProperty[String]("traitNames")
        if (s != null) {
            Seq[String](s.split(","): _*)
        } else {
            Seq()
        }
    }

    def constructInstance[U,V,E](dataType: IDataType[U], v: AnyRef): U = {
        dataType.getTypeCategory match {
            case DataTypes.TypeCategory.PRIMITIVE => dataType.convert(v, Multiplicity.OPTIONAL)
            case DataTypes.TypeCategory.ARRAY =>
                dataType.convert(v, Multiplicity.OPTIONAL)
            case DataTypes.TypeCategory.STRUCT
              if dataType.getName == TypeSystem.getInstance().getIdType.getName => {
              val sType = dataType.asInstanceOf[StructType]
              val sInstance = sType.createInstance()
              val tV = v.asInstanceOf[AtlasVertex[V,E]]
              sInstance.set(TypeSystem.getInstance().getIdType.typeNameAttrName,
                tV.getProperty[java.lang.String](typeAttributeName))
              sInstance.set(TypeSystem.getInstance().getIdType.idAttrName,
                tV.getProperty[java.lang.String](idAttributeName))
              dataType.convert(sInstance, Multiplicity.OPTIONAL)
            }
            case DataTypes.TypeCategory.STRUCT => {
                val sType = dataType.asInstanceOf[StructType]
                val sInstance = sType.createInstance()
                loadStructInstance(sType, sInstance, v.asInstanceOf[AtlasVertex[V,E]])
                dataType.convert(sInstance, Multiplicity.OPTIONAL)
            }
            case DataTypes.TypeCategory.TRAIT => {
                val tType = dataType.asInstanceOf[TraitType]
                val tInstance = tType.createInstance()
                /*
                 * this is not right, we should load the Instance associated with this trait.
                 * for now just loading the trait struct.
                 */
                loadStructInstance(tType, tInstance, v.asInstanceOf[AtlasVertex[V,E]])
                dataType.convert(tInstance, Multiplicity.OPTIONAL)
            }
            case DataTypes.TypeCategory.CLASS => {
                val cType = dataType.asInstanceOf[ClassType]
                val cInstance = constructClassInstance(dataType.asInstanceOf[ClassType], v.asInstanceOf[AtlasVertex[V,E]])
                dataType.convert(cInstance, Multiplicity.OPTIONAL)
            }
            case DataTypes.TypeCategory.ENUM => dataType.convert(v, Multiplicity.OPTIONAL)
            case x => throw new UnsupportedOperationException(s"load for ${dataType} not supported")
        }
    }

    def loadStructInstance[V,E](dataType: IConstructableType[_, _ <: ITypedInstance],
                           typInstance: ITypedInstance, v: AtlasVertex[V,E]): Unit = {
        import scala.collection.JavaConversions._
        dataType.fieldMapping().fields.foreach { t =>
            val fName = t._1
            val aInfo = t._2
            loadAttribute(dataType, aInfo, typInstance, v)
        }
    }

    def constructClassInstance[V,E](dataType: ClassType, v: AtlasVertex[V,E]): ITypedReferenceableInstance = {
        val id = getIdFromVertex(dataType.name, v)
        val tNms = traitNames(v)
        val cInstance = dataType.createInstance(id, tNms: _*)
        // load traits
        tNms.foreach { tNm =>
            val tLabel = traitLabel(dataType, tNm)
            val edges = v.getEdges(AtlasEdgeDirection.OUT, tLabel)
            val tVertex = edges.iterator().next().getInVertex().asInstanceOf[AtlasVertex[V,E]]
            val tType = TypeSystem.getInstance().getDataType[TraitType](classOf[TraitType], tNm)
            val tInstance = cInstance.getTrait(tNm).asInstanceOf[ITypedInstance]
            loadStructInstance(tType, tInstance, tVertex)
        }
        loadStructInstance(dataType, cInstance, v)
        cInstance
    }

    def loadAttribute[V,E](dataType: IDataType[_], aInfo: AttributeInfo, i: ITypedInstance, v: AtlasVertex[V,E]): Unit = {
        aInfo.dataType.getTypeCategory match {
            case DataTypes.TypeCategory.PRIMITIVE => loadPrimitiveAttribute(dataType, aInfo, i, v)
            case DataTypes.TypeCategory.ENUM => loadEnumAttribute(dataType, aInfo, i, v)
            case DataTypes.TypeCategory.ARRAY =>
                loadArrayAttribute(dataType, aInfo, i, v)
            case DataTypes.TypeCategory.MAP =>
                throw new UnsupportedOperationException(s"load for ${aInfo.dataType()} not supported")
            case DataTypes.TypeCategory.STRUCT => loadStructAttribute(dataType, aInfo, i, v)
            case DataTypes.TypeCategory.TRAIT =>
                throw new UnsupportedOperationException(s"load for ${aInfo.dataType()} not supported")
            case DataTypes.TypeCategory.CLASS => loadStructAttribute(dataType, aInfo, i, v)
        }
    }

    private def loadEnumAttribute[V,E](dataType: IDataType[_], aInfo: AttributeInfo, i: ITypedInstance, v: AtlasVertex[V,E])
    : Unit = {
        val fName = fieldNameInVertex(dataType, aInfo)
        i.setInt(aInfo.name, v.getProperty[java.lang.Integer](fName))
    }

    private def loadPrimitiveAttribute[V,E](dataType: IDataType[_], aInfo: AttributeInfo,
                                       i: ITypedInstance, v: AtlasVertex[V,E]): Unit = {
        val fName = fieldNameInVertex(dataType, aInfo)
        aInfo.dataType() match {
            case x: BooleanType => i.setBoolean(aInfo.name, v.getProperty[java.lang.Boolean](fName))
            case x: ByteType => i.setByte(aInfo.name, v.getProperty[java.lang.Byte](fName))
            case x: ShortType => i.setShort(aInfo.name, v.getProperty[java.lang.Short](fName))
            case x: IntType => i.setInt(aInfo.name, v.getProperty[java.lang.Integer](fName))
            case x: LongType => i.setLong(aInfo.name, v.getProperty[java.lang.Long](fName))
            case x: FloatType => i.setFloat(aInfo.name, v.getProperty[java.lang.Float](fName))
            case x: DoubleType => i.setDouble(aInfo.name, v.getProperty[java.lang.Double](fName))
            case x: StringType => i.setString(aInfo.name, v.getProperty[java.lang.String](fName))
            case x: DateType => {
                                  val dateVal = v.getProperty[java.lang.Long](fName)
                                  i.setDate(aInfo.name, new Date(dateVal))
                                }
            case _ => throw new UnsupportedOperationException(s"load for ${aInfo.dataType()} not supported")
        }
    }


    private def loadArrayAttribute[T,V,E](dataType: IDataType[_], aInfo: AttributeInfo,
                                    i: ITypedInstance, v: AtlasVertex[V,E]): Unit = {
        import scala.collection.JavaConversions._
        val list: java.util.List[_] = v.getProperty(aInfo.name)
        val arrayType: DataTypes.ArrayType = aInfo.dataType.asInstanceOf[ArrayType]

        var values = new util.ArrayList[Any]
        list.foreach( listElement =>
            values += mapVertexToCollectionEntry(v, aInfo, arrayType.getElemType, i, listElement)
        )
        i.set(aInfo.name, values)
    }

    private def loadStructAttribute[V,E](dataType: IDataType[_], aInfo: AttributeInfo,
                                    i: ITypedInstance, v: AtlasVertex[V,E], edgeLbl: Option[String] = None): Unit = {
        val eLabel = edgeLbl match {
            case Some(x) => x
            case None => edgeLabel(FieldInfo(dataType, aInfo, null))
        }
        val edges = v.getEdges(AtlasEdgeDirection.OUT, eLabel)
        val sVertex = edges.iterator().next().getInVertex().asInstanceOf[AtlasVertex[V,E]]
        if (aInfo.dataType().getTypeCategory == DataTypes.TypeCategory.STRUCT) {
            val sType = aInfo.dataType().asInstanceOf[StructType]
            val sInstance = sType.createInstance()
            loadStructInstance(sType, sInstance, sVertex)
            i.set(aInfo.name, sInstance)
        } else {
            val cInstance = constructClassInstance(aInfo.dataType().asInstanceOf[ClassType], sVertex)
            i.set(aInfo.name, cInstance)
        }
    }



    private def mapVertexToCollectionEntry[V,E](instanceVertex: AtlasVertex[V,E], attributeInfo: AttributeInfo, elementType: IDataType[_], i: ITypedInstance,  value: Any): Any = {
        elementType.getTypeCategory match {
            case DataTypes.TypeCategory.PRIMITIVE => value
            case DataTypes.TypeCategory.ENUM => value
            case DataTypes.TypeCategory.STRUCT =>
                throw new UnsupportedOperationException(s"load for ${attributeInfo.dataType()} not supported")
            case DataTypes.TypeCategory.TRAIT =>
                throw new UnsupportedOperationException(s"load for ${attributeInfo.dataType()} not supported")
            case DataTypes.TypeCategory.CLASS => //loadStructAttribute(elementType, attributeInfo, i, v)
                throw new UnsupportedOperationException(s"load for ${attributeInfo.dataType()} not supported")
            case _ =>
                throw new UnsupportedOperationException(s"load for ${attributeInfo.dataType()} not supported")
        }
    }
}

object Gremlin2GraphPersistenceStrategy1 extends GraphPersistenceStrategy1 {
    
    override def getSupportedGremlinVersion() : GremlinVersion =  {
        return GremlinVersion.TWO;
    }
}

object Gremlin3GraphPersistenceStrategy1 extends GraphPersistenceStrategy1 {
    
    override def getSupportedGremlinVersion() : GremlinVersion =  {
        return GremlinVersion.THREE;
    }
}


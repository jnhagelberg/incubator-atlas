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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.atlas.query.TypeUtils.FieldInfo;
import org.apache.atlas.query.Expressions._
import org.apache.atlas.repository.graphdb.GremlinVersion
import org.apache.atlas.typesystem.types.DataTypes
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory
import org.apache.atlas.typesystem.types.IDataType
import org.apache.atlas.typesystem.types.TypeSystem
import org.apache.atlas.typesystem.types.AttributeInfo
import org.joda.time.format.ISODateTimeFormat
import org.apache.atlas.typesystem.types.DataTypes.BigDecimalType
import org.apache.atlas.typesystem.types.DataTypes.ByteType
import org.apache.atlas.typesystem.types.DataTypes.BooleanType
import org.apache.atlas.typesystem.types.DataTypes.DateType
import org.apache.atlas.typesystem.types.DataTypes.BigIntegerType
import org.apache.atlas.typesystem.types.DataTypes.IntType
import org.apache.atlas.typesystem.types.DataTypes.StringType
import org.apache.atlas.typesystem.types.DataTypes.LongType
import org.apache.atlas.typesystem.types.DataTypes.DoubleType
import org.apache.atlas.typesystem.types.DataTypes.FloatType
import org.apache.atlas.typesystem.types.DataTypes.ShortType

trait IntSequence {
    def next: Int
}

case class GremlinQuery(expr: Expression, queryStr: String, resultMaping: Map[String, (String, Int)]) {

    def hasSelectList = resultMaping != null

    def isPathExpresion = expr.isInstanceOf[PathExpression]

}

trait SelectExpressionHandling {

    /**
     * To aide in gremlinQuery generation add an alias to the input of SelectExpressions
     */
    class AddAliasToSelectInput extends PartialFunction[Expression, Expression] {

        private var idx = 0

        def isDefinedAt(e: Expression) = true

        class DecorateFieldWithAlias(aliasE: AliasExpression)
            extends PartialFunction[Expression, Expression] {
            def isDefinedAt(e: Expression) = true

            def apply(e: Expression) = e match {
                case fe@FieldExpression(fieldName, fInfo, None) =>
                    FieldExpression(fieldName, fInfo, Some(BackReference(aliasE.alias, aliasE.child, None)))
                case _ => e
            }
        }

        def apply(e: Expression) = e match {
            case SelectExpression(aliasE@AliasExpression(_, _), selList) => {
                idx = idx + 1
                SelectExpression(aliasE, selList.map(_.transformUp(new DecorateFieldWithAlias(aliasE))))
            }
            case SelectExpression(child, selList) => {
                idx = idx + 1
                val aliasE = AliasExpression(child, s"_src$idx")
                SelectExpression(aliasE, selList.map(_.transformUp(new DecorateFieldWithAlias(aliasE))))
            }
            case _ => e
        }
    }

    def getSelectExpressionSrc(e: Expression): List[String] = {
        val l = ArrayBuffer[String]()
        e.traverseUp {
            case BackReference(alias, _, _) => l += alias
            case ClassExpression(clsName) => l += clsName
        }
        l.toSet.toList
    }



    def validateSelectExprHaveOneSrc: PartialFunction[Expression, Unit] = {
        case SelectExpression(_, selList) => {
            selList.foreach { se =>
                val srcs = getSelectExpressionSrc(se)
                if (srcs.size > 1) {
                    throw new GremlinTranslationException(se, "Only one src allowed in a Select Expression")
                }
            }
        }
    }

    def groupSelectExpressionsBySrc(sel: SelectExpression): mutable.LinkedHashMap[String, List[Expression]] = {
        val m = mutable.LinkedHashMap[String, List[Expression]]()
        sel.selectListWithAlias.foreach { se =>
            val l = getSelectExpressionSrc(se.child)
            if (!m.contains(l(0))) {
                m(l(0)) = List()
            }
            m(l(0)) = m(l(0)) :+ se.child
        }
        m
    }

    /**
     * For each Output Column in the SelectExpression compute the ArrayList(Src) this maps to and the position within
     * this list.
     * @param sel
     * @return
     */
    def buildResultMapping(sel: SelectExpression): Map[String, (String, Int)] = {
        val srcToExprs = groupSelectExpressionsBySrc(sel)
        val m = new mutable.HashMap[String, (String, Int)]
        sel.selectListWithAlias.foreach { se =>
            val src = getSelectExpressionSrc(se.child)(0)
            val srcExprs = srcToExprs(src)
            var idx = srcExprs.indexOf(se.child)
            m(se.alias) = (src, idx)
        }
        m.toMap
    }
}

class GremlinTranslationException(expr: Expression, reason: String) extends
ExpressionException(expr, s"Unsupported Gremlin translation: $reason")

class GremlinTranslator(expr: Expression,
                        gPersistenceBehavior: GraphPersistenceStrategies)
    extends SelectExpressionHandling {

    val preStatements = ArrayBuffer[String]()
    val postStatements = ArrayBuffer[String]()

    val wrapAndRule: PartialFunction[Expression, Expression] = {
        case f: FilterExpression if !f.condExpr.isInstanceOf[LogicalExpression] =>
            FilterExpression(f.child, new LogicalExpression("and", List(f.condExpr)))
    }

    val validateComparisonForm: PartialFunction[Expression, Unit] = {
        case c@ComparisonExpression(op, left, right) =>
            if (!left.isInstanceOf[FieldExpression]) {
                throw new GremlinTranslationException(c, s"lhs of comparison is not a field")
            }
            if (!right.isInstanceOf[Literal[_]] && !right.isInstanceOf[ListLiteral[_]]) {
                throw new GremlinTranslationException(c,
                    s"rhs of comparison is not a literal")
            }

            if(right.isInstanceOf[ListLiteral[_]] && (!op.equals("=") && !op.equals("!="))) {
                throw new GremlinTranslationException(c,
                    s"operation not supported with list literal")
            }
            ()
    }

    val counter =  new IntSequence {
        var i: Int = -1;

        def next: Int = {
            i += 1; i
        }
    }

    def addAliasToLoopInput(c: IntSequence = counter): PartialFunction[Expression, Expression] = {
        case l@LoopExpression(aliasE@AliasExpression(_, _), _, _) => l
        case l@LoopExpression(inputExpr, loopExpr, t) => {
            val aliasE = AliasExpression(inputExpr, s"_loop${c.next}")
            LoopExpression(aliasE, loopExpr, t)
        }
    }

    def instanceClauseToTop(topE : Expression) : PartialFunction[Expression, Expression] = {
      case le : LogicalExpression if (le fastEquals topE) =>  {
        le.instance()
      }
      case ce : ComparisonExpression if (ce fastEquals topE) =>  {
        ce.instance()
      }
      case he : hasFieldUnaryExpression if (he fastEquals topE) =>  {
        he.instance()
      }
    }

    def traitClauseWithInstanceForTop(topE : Expression) : PartialFunction[Expression, Expression] = {
//          This topE check prevented the comparison of trait expression when it is a child. Like trait as t limit 2
        case te : TraitExpression =>  {
        val theTrait = te.as("theTrait")
        val theInstance = theTrait.traitInstance().as("theInstance")
        val outE =
          theInstance.select(id("theTrait").as("traitDetails"),
            id("theInstance").as("instanceInfo"))
        QueryProcessor.validate(outE)
      }
    }

    def typeTestExpression(typeName : String) : String = {
        val stats = gPersistenceBehavior.typeTestExpression(typeName, counter)
        preStatements ++= stats.init
        stats.last
    }


    private def genQuery(expr: Expression, inSelect: Boolean): String = expr match {
        case ClassExpression(clsName) =>
            typeTestExpression(clsName)
        case TraitExpression(clsName) =>
            typeTestExpression(clsName)
        case fe@FieldExpression(fieldName, fInfo, child)
            if fe.dataType.getTypeCategory == TypeCategory.PRIMITIVE || fe.dataType.getTypeCategory == TypeCategory.ARRAY => {
            val fN = "\"" + gPersistenceBehavior.fieldNameInVertex(fInfo.dataType, fInfo.attrInfo) + "\""
            child match {
                case Some(e) => genPropertyAccessExpr(e, fInfo, fN, inSelect)
                case None => s"$fN"
            }
        }
        case fe@FieldExpression(fieldName, fInfo, child)
            if fe.dataType.getTypeCategory == TypeCategory.CLASS || fe.dataType.getTypeCategory == TypeCategory.STRUCT => {
            val direction = if (fInfo.isReverse) "in" else "out"
            val edgeLbl = gPersistenceBehavior.edgeLabel(fInfo)
            val step = s"""$direction("$edgeLbl")"""
            child match {
                case Some(e) => s"${genQuery(e, inSelect)}.$step"
                case None => step
            }
        }
        case fe@FieldExpression(fieldName, fInfo, child) if fInfo.traitName != null => {
            val direction = gPersistenceBehavior.instanceToTraitEdgeDirection
            val edgeLbl = gPersistenceBehavior.edgeLabel(fInfo)
            val step = s"""$direction("$edgeLbl")"""
            child match {
              case Some(e) => s"${genQuery(e, inSelect)}.$step"
              case None => step
            }
        }
        case c@ComparisonExpression(symb, f@FieldExpression(fieldName, fInfo, ch), l) => {
          val QUOTE = "\"";
          val fieldGremlinExpr = s"${gPersistenceBehavior.fieldNameInVertex(fInfo.dataType, fInfo.attrInfo)}"
            ch match {
                case Some(child) => {
                  s"""${genQuery(child, inSelect)}.${genHasPredicate(fieldGremlinExpr,c, l)}"""
                }
                case None => {
                    if (fInfo.attrInfo.dataType == DataTypes.DATE_TYPE) {
                        try {
                            //Accepts both date, datetime formats
                            val dateStr = l.toString.stripPrefix(QUOTE).stripSuffix(QUOTE)
                            val dateVal = ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(dateStr).getMillis
                            return genHasPredicate(fieldGremlinExpr, c, dateVal);
                        } catch {
                            case pe: java.text.ParseException =>
                                throw new GremlinTranslationException(c,
                                    "Date format " + l + " not supported. Should be of the format " + TypeSystem.getInstance().getDateFormat.toPattern);

                        }
                    }
                    else {
                        return genHasPredicate(fieldGremlinExpr, c, l);
                    }
                }
            }
        }
        case fil@FilterExpression(child, condExpr) => {
            s"${genQuery(child, inSelect)}.${genQuery(condExpr, inSelect)}"
        }
        case l@LogicalExpression(symb, children) => {
            if(gPersistenceBehavior.getSupportedGremlinVersion() == GremlinVersion.THREE) {
                if(children.length == 1) {
                    //gremlin 3 treats one element expressions as 'false'.  Avoid
                    //creating a boolean expression in this case.  Inline the expression
                    //note: we can't simply omit it, since it will cause us to traverse the edge!
                    //use 'where' instead
                    var child : Expression = children.head;
                    //if child is a back expression, that expression becomes an argument to where

                    return s"""where(${genQuery(child, inSelect)})""";
                }
                else {
                    // Gremlin 3 does not support _() syntax
                    //
                   return s"""$symb${children.map( genQuery(_, inSelect)).mkString("(", ",", ")")}"""
                }
           }
           else {
                s"""$symb${children.map("_()." + genQuery(_, inSelect)).mkString("(", ",", ")")}"""
           }
        }
        case sel@SelectExpression(child, selList) => {
              val m = groupSelectExpressionsBySrc(sel)
              var srcNamesList: List[String] = List()
              var srcExprsList: List[List[String]] = List()
              val it = m.iterator
              while (it.hasNext) {
                  val (src, selExprs) = it.next
                  srcNamesList = srcNamesList :+ s""""$src""""
                  srcExprsList = srcExprsList :+ selExprs.map { selExpr =>
                      genQuery(selExpr, true)
                   }
               }
               val srcExprsStringList = srcExprsList.map {
                    _.mkString("[", ",", "]")
               }

               if(gPersistenceBehavior.getSupportedGremlinVersion() == GremlinVersion.TWO) {
                    val srcNamesString = srcNamesList.mkString("[", ",", "]")
                    val srcExprsString = srcExprsStringList.foldLeft("")(_ + "{" + _ + "}")
                    s"${genQuery(child, inSelect)}.select($srcNamesString)$srcExprsString"
               }
               else {
                    //gremlin 3
                    val srcNamesString = srcNamesList.mkString("", ",", "")
                    val srcExprsString = srcExprsStringList.foldLeft("")(_ + ".by({" + _ + "} as Function)")
                    System.out.println("srcNamesList: " + srcNamesList);
                    System.out.println("srcExprsList: " + srcExprsList);

                    s"${genQuery(child, inSelect)}.select($srcNamesString)$srcExprsString"
               }
        }
        case loop@LoopExpression(input, loopExpr, t) => {

            if(gPersistenceBehavior.getSupportedGremlinVersion() == GremlinVersion.TWO) {
               val inputQry = genQuery(input, inSelect)
               val loopingPathGExpr = genQuery(loopExpr, inSelect)
               val loopGExpr = s"""loop("${input.asInstanceOf[AliasExpression].alias}")"""
               val untilCriteria = if (t.isDefined) s"{it.loops < ${t.get.value}}" else "{true}"
               val loopObjectGExpr = gPersistenceBehavior.loopObjectExpression(input.dataType)
               s"""${inputQry}.${loopingPathGExpr}.${loopGExpr}${untilCriteria}${loopObjectGExpr}"""
            }
            else {
                //gremlin 3
               val inputQry = genQuery(input, inSelect)
               val repeatExpr = s"""repeat(__.${genQuery(loopExpr, inSelect)})"""
               val optTimesExpr = if (t.isDefined) s".times(${t.get.value})" else ""
               val emitExpr = s""".emit(${gPersistenceBehavior.loopObjectExpression(input.dataType)})"""

               s"""${inputQry}.${repeatExpr}${optTimesExpr}${emitExpr}"""

            }
        }
        case BackReference(alias, _, _) => {

            if (inSelect) {
                gPersistenceBehavior.fieldPrefixInSelect()
            } else {
                if(gPersistenceBehavior.getSupportedGremlinVersion() == GremlinVersion.TWO) {
                    s"""back("$alias")"""
                }
                else {
                    s"""select("$alias")"""
                }
            }
        }
        case AliasExpression(child, alias) => s"""${genQuery(child, inSelect)}.as("$alias")"""
        case isTraitLeafExpression(traitName, Some(clsExp)) =>
            s"""out("${gPersistenceBehavior.traitLabel(clsExp.dataType, traitName)}")"""
        case isTraitUnaryExpression(traitName, child) =>
            s"""out("${gPersistenceBehavior.traitLabel(child.dataType, traitName)}")"""
        case hasFieldLeafExpression(fieldName, clsExp) => clsExp match {
            case None => s"""has("$fieldName")"""
            case Some(x) => {
                val fi = TypeUtils.resolveReference(clsExp.get.dataType, fieldName);
                if(! fi.isDefined) {
                    s"""has("$fieldName")"""
                }
                else {
                    s"""has("${gPersistenceBehavior.fieldNameInVertex(fi.get.dataType, fi.get.attrInfo)}")"""
                }
            }
        }
        case hasFieldUnaryExpression(fieldName, child) =>
            s"""${genQuery(child, inSelect)}.has("$fieldName")"""
        case ArithmeticExpression(symb, left, right) => s"${genQuery(left, inSelect)} $symb ${genQuery(right, inSelect)}"
        case l: Literal[_] => l.toString
        case list: ListLiteral[_] => list.toString
        case in@TraitInstanceExpression(child) => {
          val direction = gPersistenceBehavior.traitToInstanceEdgeDirection
          s"${genQuery(child, inSelect)}.$direction()"
        }
        case in@InstanceExpression(child) => {
          s"${genQuery(child, inSelect)}"
        }
        case pe@PathExpression(child) => {
            if(gPersistenceBehavior.getSupportedGremlinVersion() == GremlinVersion.TWO) {
                 s"${genQuery(child, inSelect)}.path"
            }
            else {
                s"${genQuery(child, inSelect)}.path()"
            }
        }
        case order@OrderExpression(child, odr, asc) => {
          var orderby = ""
          if(gPersistenceBehavior.getSupportedGremlinVersion() == GremlinVersion.TWO) {
              asc  match {
               //builds a closure comparison function based on provided order by clause in DSL. This will be used to sort the results by gremlin order pipe.
              //Ordering is case insensitive.
              case false=> orderby = s"order{it.b.getProperty('$odr').toLowerCase() <=> it.a.getProperty('$odr').toLowerCase()}"//descending
              case _ => orderby = s"order{it.a.getProperty('$odr').toLowerCase() <=> it.b.getProperty('$odr').toLowerCase()}"

            }
          }
          else {

               asc  match {
               //builds a closure comparison function based on provided order by clause in DSL. This will be used to sort the results by gremlin order pipe.
              //Ordering is case insensitive.
              case false=> orderby = s"order().by('$odr',{ a,b -> b.toString().toLowerCase() <=> a.toString().toLowerCase() })"
              case _ => orderby = s"order().by('$odr',{ a,b -> a.toString().toLowerCase() <=> b.toString().toLowerCase() })"

            }

          }
          s"""${genQuery(child, inSelect)}.$orderby"""
        }
        case limitOffset@LimitExpression(child, limit, offset) => {
            if(gPersistenceBehavior.getSupportedGremlinVersion() == GremlinVersion.TWO) {
                val totalResultRows = limit.value + offset.value
                s"""${genQuery(child, inSelect)} [$offset..<$totalResultRows]"""
            }
            else {
                val totalResultRows = limit.value + offset.value
                s"""${genQuery(child, inSelect)}.range($offset,$totalResultRows)"""

            }
        }
        case x => throw new GremlinTranslationException(x, "expression not yet supported")
    }

    def genPropertyAccessExpr(e: Expression, fInfo : FieldInfo, propertyName: String, inSelect: Boolean) : String = {
        
        if(gPersistenceBehavior.getSupportedGremlinVersion() == GremlinVersion.TWO) {
            s"${genQuery(e, inSelect)}.$propertyName"
        }
        else {
            val attrInfo : AttributeInfo = fInfo.attrInfo;
            val attrType : IDataType[_] = attrInfo.dataType;
            if(inSelect) {
                val expr = s"${genQuery(e, inSelect)}.value($propertyName)";
                return addTypeCast(expr, attrType);
                
            }
            else {
                 s"${genQuery(e, inSelect)}.values($propertyName)"
            }
        }
    }

    def genHasPredicate(fieldGremlinExpr: String, c: ComparisonExpression, expr2: Any) : String = {

            if(gPersistenceBehavior.getSupportedGremlinVersion() == GremlinVersion.TWO) {
                return s"""has("${fieldGremlinExpr}", ${gPersistenceBehavior.gremlinCompOp(c)}, $expr2)""";
            }
            else {
                return s"""has("${fieldGremlinExpr}", ${gPersistenceBehavior.gremlinCompOp(c)}($expr2))""";
            }
    }

    def addTypeCast(expr: String, t: IDataType[_]) : String = {
      //use "as" syntax since the value might be stored using a
      //different representation in the underlying graph database.  This
      //coerces the value into the type we need.
      if(getPrimitiveTypeQualifier.isDefinedAt(t)) {
          return """(${expr} as ${getPrimitiveTypeQualifier(t)})"""
      }
      else {
        return expr;
      }      
    }
    
    def getPrimitiveTypeQualifier : PartialFunction[IDataType[_],String]  = {

    case t:BooleanType => "Boolean";
    case t:ByteType => "Byte";
    case t:DateType => "Long"; //dates are stored numerically
    case t:DoubleType => "Double";
    case t:FloatType => "Float";
    case t:IntType => "Integer";
    case t:LongType => "Long";
    case t:ShortType => "Short";
    case t:StringType => "String";
}

    def genFullQuery(expr: Expression): String = {
        var q = genQuery(expr, false)
        if(gPersistenceBehavior.addGraphVertexPrefix(preStatements)) {
            q = s"g.V.$q"
        }

        q = s"$q.toList()"

        q = (preStatements ++ Seq(q) ++ postStatements).mkString("", ";", "")
        /*
         * the L:{} represents a groovy code block; the label is needed
         * to distinguish it from a groovy closure.
         */
        s"L:{$q}"
    }

    def translate(): GremlinQuery = {
        var e1 = expr.transformUp(wrapAndRule)

        e1.traverseUp(validateComparisonForm)
        e1 = e1.transformUp(new AddAliasToSelectInput)
        e1.traverseUp(validateSelectExprHaveOneSrc)
        e1 = e1.transformUp(addAliasToLoopInput())
        e1 = e1.transformUp(instanceClauseToTop(e1))
        e1 = e1.transformUp(traitClauseWithInstanceForTop(e1))

       //Following code extracts the select expressions from expression tree.

             val  se = SelectExpressionHelper.extractSelectExpression(e1)
             if (se.isDefined)
             {
                val  rMap = buildResultMapping(se.get)
                GremlinQuery(e1, genFullQuery(e1), rMap)
             }
             else
             {
                GremlinQuery(e1, genFullQuery(e1), null)
             }
        }

    }

    object SelectExpressionHelper {
       /**
     * This method extracts the child select expression from parent expression
     */
     def extractSelectExpression(child: Expression): Option[SelectExpression] = {
      child match {
            case se@SelectExpression(child, selectList) =>{
              Some(se)
            }
            case limit@LimitExpression(child, lmt, offset) => {
              extractSelectExpression(child)
            }
            case order@OrderExpression(child, odr, odrBy) => {
              extractSelectExpression(child)
            }
           case path@PathExpression(child) => {
              extractSelectExpression(child)
           }
           case _ => {
             None
           }

      }
    }
    }
    /*
     * TODO
     * Translation Issues:
     * 1. back references in filters. For e.g. testBackreference: 'DB as db Table where (db.name = "Reporting")'
     *    this is translated to:
     * g.V.has("typeName","DB").as("db").in("Table.db").and(_().back("db").has("name", T.eq, "Reporting")).map().toList()
     * But the '_().back("db") within the and is ignored, the has condition is applied on the current element.
     * The solution is to to do predicate pushdown and apply the filter immediately on top of the referred Expression.
     */


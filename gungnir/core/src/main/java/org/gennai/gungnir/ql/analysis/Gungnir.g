/**
 * Copyright 2013-2014 Recruit Technologies Co., Ltd. and contributors
 * (see CONTRIBUTORS.md)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  A copy of the
 * License is distributed with this work in the LICENSE.md file.  You may
 * also obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
grammar Gungnir;

options {
  output       = AST;
  ASTLabelType = CommonTree;
  backtrack    = true;
}

tokens {
  TOK_QUERY;
  TOK_FROM;
  TOK_SPOUT;
  TOK_SCHEMA_EXPRS;
  TOK_SCHEMA_EXPR;
  TOK_JOIN_SCHEMA_EXPRS;
  TOK_JOIN_SCHEMA_EXPR;
  TOK_TUPLE_JOIN_FIELD;
  TOK_STREAM;
  TOK_STREAM_EXPRS;
  TOK_STREAM_EXPR;
  TOK_TUPLE_NAMES;
  TOK_JOIN_STREAM_EXPRS;
  TOK_JOIN_STREAM_EXPR;
  TOK_TUPLE_JOIN_TO;
  TOK_EMIT;
  TOK_INTO;
  TOK_JOIN;
  TOK_JOIN_TO;
  TOK_EACH;
  TOK_EACH_EXPRS;
  TOK_EACH_EXPR;
  TOK_CAST;
  TOK_DISTINCT;
  TOK_FILTER;
  TOK_FILTER_GROUP;
  TOK_SLIDE;
  TOK_SNAPSHOT;
  TOK_AGGREGATE_EXPRS;
  TOK_AGGREGATE_EXPR;
  TOK_LIMIT;
  TOK_FIRST;
  TOK_LAST;
  TOK_BEGIN_GROUP;
  TOK_END_GROUP;
  TOK_TO_STREAM;
  TOK_PARTITION_BY;
  TOK_EXPLAIN;
  TOK_EXTENDED;
  TOK_CLEAR;
  TOK_CREATE_TUPLE;
  TOK_FIELD_DEFS;
  TOK_FIELD_DEF;
  TOK_PARTITIONED_BY;
  TOK_COMMENT;
  TOK_DESC_TUPLE;
  TOK_SHOW_TUPLES;
  TOK_DROP_TUPLE;
  TOK_CREATE_VIEW;
  TOK_DESC_VIEW;
  TOK_SHOW_VIEWS;
  TOK_DROP_VIEW;
  TOK_CREATE_FUNCTION;
  TOK_DESC_FUNCTION;
  TOK_SHOW_FUNCTIONS;
  TOK_DROP_FUNCTION;
  TOK_CREATE_USER;
  TOK_DESC_USER;
  TOK_SHOW_USERS;
  TOK_ALTER_USER;
  TOK_DROP_USER;
  TOK_SUBMIT_TOPOLOGY;
  TOK_DESC_TOPOLOGY;
  TOK_SHOW_TOPOLOGIES;
  TOK_STOP_TOPOLOGY;
  TOK_START_TOPOLOGY;
  TOK_DROP_TOPOLOGY;
  TOK_STATS_TOPOLOGY;
  TOK_DESC_CLUSTER;
  TOK_FIELD;
  TOK_SUBSCRIPT;
  TOK_FIELDS;
  TOK_ARRAY;
  TOK_MAP;
  TOK_CONDITION;
  TOK_OP_EQ;
  TOK_OP_NE;
  TOK_OP_LE;
  TOK_OP_LT;
  TOK_OP_GE;
  TOK_OP_GT;
  TOK_OP_LIKE;
  TOK_OP_REGEXP;
  TOK_OP_IN;
  TOK_OP_ALL;
  TOK_OP_BETWEEN;
  TOK_OP_IS_NULL;
  TOK_OP_IS_NOT_NULL;
  TOK_OP_OR;
  TOK_OP_AND;
  TOK_OP_NOT;
  TOK_CONDITIONS;
  TOK_PROCESSOR;
  TOK_FUNCTION;
  TOK_ARGUMENTS;
  TOK_COUNT_INTERVAL;
  TOK_TIME_INTERVAL;
  TOK_CRON_INTERVAL;
  TOK_PARALLELISM;
}

@header {
package org.gennai.gungnir.ql.analysis;
}

@lexer::header {
package org.gennai.gungnir.ql.analysis;
}

@members {
private final Stack msgs = new Stack<String>();
private final ArrayList<ParseError> errors = new ArrayList<ParseError>();

@Override
public Object recoverFromMismatchedSet(IntStream input, RecognitionException e,
		BitSet follow) throws RecognitionException {
	throw e;
}

@Override
public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
	errors.add(new ParseError(this, e, tokenNames));
}

@Override
public String getErrorHeader(RecognitionException e) {
	String header = null;
	if (e.charPositionInLine < 0 && input.LT(-1) != null) {
		Token t = input.LT(-1);
		header = "line " + t.getLine() + ":" + t.getCharPositionInLine();
	} else {
		header = super.getErrorHeader(e);
	}
	return header;
}

@Override
public String getErrorMessage(RecognitionException e, String[] tokenNames) {
	String msg = null;

	if (e instanceof NoViableAltException) {
		msg = "cannot recognize input near"
				+ (input.LT(1) != null ? " "
						+ getTokenErrorDisplay(input.LT(1)) : "")
				+ (input.LT(2) != null ? " "
						+ getTokenErrorDisplay(input.LT(2)) : "")
				+ (input.LT(3) != null ? " "
						+ getTokenErrorDisplay(input.LT(3)) : "");
	} else if (e instanceof MismatchedTokenException) {
		msg = super.getErrorMessage(e, tokenNames)
				+ (input.LT(-1) == null ? "" : " near '"
						+ input.LT(-1).getText()) + "'";
	} else if (e instanceof FailedPredicateException) {
		FailedPredicateException fpe = (FailedPredicateException) e;
		msg = "failed to recognize predicate '" + fpe.token.getText()
				+ "'. failed rule: '" + fpe.ruleName + "'";
	} else {
		msg = super.getErrorMessage(e, tokenNames);
	}

	if (msgs.size() > 0) {
		msg = msg + " in " + msgs.peek();
	}
	return msg;
}

public ArrayList<ParseError> getErrors() {
	return errors;
}
}

@lexer::members {
private final ArrayList<ParseError> errors = new ArrayList<ParseError>();

@Override
public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
	errors.add(new ParseError(this, e, tokenNames));
}

@Override
public String getErrorMessage(RecognitionException e, String[] tokenNames) {
	String msg = null;

	if (e instanceof NoViableAltException) {
		msg = "character " + getCharErrorDisplay(e.c) + " not supported here";
	} else {
		msg = super.getErrorMessage(e, tokenNames);
	}

	return msg;
}

public ArrayList<ParseError> getErrors() {
	return errors;
}
}

@rulecatch {
catch (RecognitionException e) {
  reportError(e);
  throw e;
}
}

statement
  :
  dmlStatement EOF
  | ddlStatement EOF
  | tclStatement EOF
  ;

dmlStatement
@init {
msgs.push("DML statement");
}
@after {
msgs.pop();
}
  :
  | queryStatement
  | explainStatement
  | clearStatement
  ;

queryStatement
@init {
msgs.push("query statement");
}
@after {
msgs.pop();
}
  :
  fromClause operation+
    ->
      ^(TOK_QUERY fromClause operation+)
  ;

fromClause
@init {
msgs.push("from clause");
}
@after {
msgs.pop();
}
  :
  FROM
  (
    fromSpout
    | fromStreams
  )
    ->
      ^(TOK_FROM fromSpout? fromStreams?)
  ;

fromSpout
  :
  schemaExprs USING processor parallelism?
    ->
      ^(TOK_SPOUT schemaExprs processor parallelism?)
  ;

schemaExprs
  :
  schemaExpr (COMMA schemaExpr)*
    ->
      ^(TOK_SCHEMA_EXPRS schemaExpr+)
  ;

schemaExpr
  :
  schemaName (AS tupleName)?
    ->
      ^(TOK_SCHEMA_EXPR schemaName tupleName?)
  | LPAREN schemaName joinSchemaExprs tupleJoinToExpr EXPIRE period (USING processor)? RPAREN AS tupleName
    ->
      ^(TOK_SCHEMA_EXPR schemaName joinSchemaExprs tupleJoinToExpr period processor? tupleName)
  ;

joinSchemaExprs
  :
  joinSchemaExpr+
    ->
      ^(TOK_JOIN_SCHEMA_EXPRS joinSchemaExpr+)
  ;

joinSchemaExpr
  :
  JOIN schemaName ON tupleJoinCondition parallelism?
    ->
      ^(TOK_JOIN_SCHEMA_EXPR schemaName tupleJoinCondition parallelism?)
  ;

fromStreams
  :
  streamExprs
    ->
      ^(TOK_STREAM streamExprs)
  ;

streamExprs
  :
  streamExpr (COMMA streamExpr)*
    ->
      ^(TOK_STREAM_EXPRS streamExpr+)
  ;

streamExpr
  :
  streamName
    ->
      ^(TOK_STREAM_EXPR streamName)
  | streamName LPAREN tupleNames RPAREN (AS tupleName)?
    ->
      ^(TOK_STREAM_EXPR streamName tupleNames tupleName?)
  | LPAREN streamName LPAREN tupleName RPAREN joinStreamExprs tupleJoinToExpr EXPIRE period (USING processor)? RPAREN AS tupleName
    ->
      ^(TOK_STREAM_EXPR streamName tupleName joinStreamExprs tupleJoinToExpr period processor? tupleName)
  ;

tupleNames
  :
  tupleName (COMMA tupleName)*
    ->
      ^(TOK_TUPLE_NAMES tupleName+)
  ;

joinStreamExprs
  :
  joinStreamExpr+
    ->
      ^(TOK_JOIN_STREAM_EXPRS joinStreamExpr+)
  ;

joinStreamExpr
  :
  JOIN streamName LPAREN tupleName RPAREN ON tupleJoinCondition
    ->
      ^(TOK_JOIN_STREAM_EXPR streamName tupleName tupleJoinCondition)
  ;

tupleJoinCondition
  :
  equalCondition (and=AND equalCondition)*
    -> {$and != null}?
      ^(TOK_OP_AND equalCondition+)
    -> equalCondition
  ;

equalCondition
  :
  field EQUAL field
    ->
      ^(TOK_OP_EQ field field)
  ;

tupleJoinToExpr
  :
  TO tupleJoinField (COMMA tupleJoinField)*
    ->
      ^(TOK_TUPLE_JOIN_TO tupleJoinField+)
  ;

tupleJoinField
  :
  tupleName DOT
  (
    fieldName
    | ASTERISK
  )
  (AS aliasFieldName)?
    ->
      ^(TOK_TUPLE_JOIN_FIELD tupleName fieldName? ASTERISK? aliasFieldName?)
  ;

operation
  :
  emitClause
  | intoClause
  | joinClause
  | eachClause
  | filterClause
  | filterGroupClause
  | slideClause
  | snapshotClause
  | limitClause
  | beginGroupClause
  | endGroupClause
  | toStreamClause
  | partitionByClause
  ;

emitClause
@init {
msgs.push("emit clause");
}
@after {
msgs.pop();
}
  :
  EMIT fields
  (
    USING processor
    | TO schemaName
  )
  parallelism?
    ->
      ^(TOK_EMIT fields processor? schemaName? parallelism?)
  ;

fields
  :
  field (COMMA field)*
    ->
      ^(TOK_FIELDS field+)
  ;

intoClause
@init {
msgs.push("into clause");
}
@after {
msgs.pop();
}
  :
  INTO streamName
    ->
      ^(TOK_INTO streamName)
  ;

joinClause
@init {
msgs.push("join clause");
}
@after {
msgs.pop();
}
  :
  JOIN joinToExpr USING processor parallelism?
    ->
      ^(TOK_JOIN joinToExpr processor parallelism?)
  ;

joinToExpr
  :
  fieldName (COMMA fieldName)*
    ->
      ^(TOK_JOIN_TO fieldName+)
  ;

eachClause
@init {
msgs.push("each clause");
}
@after {
msgs.pop();
}
  :
  EACH eachExprs parallelism?
    ->
      ^(TOK_EACH eachExprs parallelism?)
  ;

eachExprs
  :
  eachExpr (COMMA eachExpr)*
    ->
      ^(TOK_EACH_EXPRS eachExpr+)
  ;

eachExpr
  :
  arith AS aliasFieldName
    ->
      ^(TOK_EACH_EXPR arith aliasFieldName)
  | field (AS aliasFieldName)?
    ->
      ^(TOK_EACH_EXPR field aliasFieldName?)
  | cast (AS aliasFieldName)?
    ->
      ^(TOK_EACH_EXPR cast aliasFieldName?)
  ;

cast
  :
  CAST LPAREN
  (
    constant
    | arith
  )
  AS primitiveType RPAREN
    ->
      ^(TOK_CAST arith? constant? primitiveType)
  ;

filterClause
@init {
msgs.push("filter clause");
}
@after {
msgs.pop();
}
  :
  FILTER condition parallelism?
    ->
      ^(TOK_FILTER condition parallelism?)
  ;

filterGroupClause
@init {
msgs.push("filter group clause");
}
@after {
msgs.pop();
}
  :
  FILTER GROUP EXPIRE period (STATE TO fieldName)? conditions parallelism?
    ->
      ^(TOK_FILTER_GROUP period fieldName? conditions parallelism?)
  ;

conditions
  :
  condition (COMMA condition)*
    ->
      ^(TOK_CONDITIONS condition+)
  ;

slideClause
@init {
msgs.push("slide clause");
}
@after {
msgs.pop();
}
  :
  SLIDE LENGTH
  (
    countInterval
    | (timeInterval BY field)
  )
  aggregateExprs parallelism?
    ->
      ^(TOK_SLIDE countInterval? timeInterval? field? aggregateExprs parallelism?)
  ;

snapshotClause
@init {
msgs.push("snapshot clause");
}
@after {
msgs.pop();
}
  :
  SNAPSHOT EVERY
  (
    countInterval
    | timeInterval
    | cronInterval
  )
  aggregateExprs snapshotExpire? parallelism?
    ->
      ^(TOK_SNAPSHOT countInterval? timeInterval? cronInterval? aggregateExprs snapshotExpire? parallelism?)
  ;

aggregateExprs
  :
  aggregateExpr (COMMA aggregateExpr)*
    ->
      ^(TOK_AGGREGATE_EXPRS aggregateExpr+)
  ;

aggregateExpr
  :
  arith AS aliasFieldName
    ->
      ^(TOK_AGGREGATE_EXPR arith aliasFieldName)
  | field (AS aliasFieldName)?
    ->
      ^(TOK_AGGREGATE_EXPR field aliasFieldName?)
  ;

snapshotExpire
  :
  EXPIRE
  (
    countInterval
    | timeInterval
    | cronInterval
  )
    -> countInterval? timeInterval? cronInterval?
  ;

limitClause
@init {
msgs.push("limit clause");
}
@after {
msgs.pop();
}
  :
  LIMIT
  (
    limitFirst
    | limitLast
  )
  parallelism?
    ->
      ^(TOK_LIMIT limitFirst? limitLast? parallelism?)
  ;

limitFirst
  :
  FIRST EVERY
  (
    countInterval
    | timeInterval
  )
    ->
      ^(TOK_FIRST countInterval? timeInterval?)
  ;

limitLast
  :
  LAST EVERY
  (
    countInterval
    | timeInterval
  )
    ->
      ^(TOK_LAST countInterval? timeInterval?)
  ;

beginGroupClause
@init {
msgs.push("group clause");
}
@after {
msgs.pop();
}
  :
  BEGIN GROUP BY fields
    ->
      ^(TOK_BEGIN_GROUP fields)
  ;

endGroupClause
@init {
msgs.push("group clause");
}
@after {
msgs.pop();
}
  :
  END GROUP
    -> TOK_END_GROUP
  ;

toStreamClause
@init {
msgs.push("to stream clause");
}
@after {
msgs.pop();
}
  :
  TO STREAM
    -> TOK_TO_STREAM
  ;

partitionByClause
@init {
msgs.push("partition by clause");
}
@after {
msgs.pop();
}
  :
  PARTITION BY fields
    ->
      ^(TOK_PARTITION_BY fields)
  ;

explainStatement
@init {
msgs.push("explain statement");
}
@after {
msgs.pop();
}
  :
  EXPLAIN extended? topologyName?
    ->
      ^(TOK_EXPLAIN extended? topologyName?)
  ;

extended
  :
  EXTENDED
    -> TOK_EXTENDED
  ;

clearStatement
@init {
msgs.push("clear statement");
}
@after {
msgs.pop();
}
  :
  CLEAR
    -> TOK_CLEAR
  ;

tupleName
  :
  identifier
  ;

streamName
  :
  identifier
  ;

fieldName
  :
  identifier
  ;

aliasFieldName
  :
  identifier
  ;

ddlStatement
@init {
msgs.push("DDL statement");
}
@after {
msgs.pop();
}
  :
  createTupleStatement
  | descTupleStatement
  | showTuplesStatement
  | dropTupleStatement
  | createViewStatement
  | descViewStatement
  | showViewsStatement
  | dropViewStatement
  | createFunctionStatement
  | descFunctionStatement
  | showFunctionsStatement
  | dropFunctionStatement
  | createUserStatement
  | descUserStatement
  | showUsersStatement
  | alterUserStatement
  | dropUserStatement
  ;

createTupleStatement
@init {
msgs.push("create tuple statement");
}
@after {
msgs.pop();
}
  :
  CREATE TUPLE schemaName LPAREN fieldDefines RPAREN partitionedByExpr? commentExpr?
    ->
      ^(TOK_CREATE_TUPLE schemaName fieldDefines partitionedByExpr? commentExpr?)
  ;

fieldDefines
  :
  fieldDefine (COMMA fieldDefine)*
    ->
      ^(TOK_FIELD_DEFS fieldDefine+)
  ;

fieldDefine
  :
  fieldName fieldType?
    ->
      ^(TOK_FIELD_DEF fieldName fieldType?)
  ;

fieldType
  :
  primitiveType
  | LIST^ LESS! primitiveType GREATER!
  | MAP^ LESS! primitiveType COMMA! primitiveType GREATER!
  | STRUCT^ LESS! fieldDefines GREATER!
  ;

primitiveType
  :
  STRING
  | TINYINT
  | SMALLINT
  | INT
  | BIGINT
  | FLOAT
  | DOUBLE
  | BOOLEAN
  | TIMESTAMP^ (LPAREN! dateFormat RPAREN!)?
  ;

dateFormat
  :
  StringLiteral
  ;

partitionedByExpr
  :
  PARTITIONED BY fieldName (COMMA fieldName)*
    ->
      ^(TOK_PARTITIONED_BY fieldName+)
  ;

descTupleStatement
@init {
msgs.push("describe tuple statement");
}
@after {
msgs.pop();
}
  :
  DESC TUPLE schemaName
    ->
      ^(TOK_DESC_TUPLE schemaName)
  ;

showTuplesStatement
@init {
msgs.push("show tuples statement");
}
@after {
msgs.pop();
}
  :
  SHOW TUPLES
    -> TOK_SHOW_TUPLES
  ;

dropTupleStatement
@init {
msgs.push("drop tuple statement");
}
@after {
msgs.pop();
}
  :
  DROP TUPLE schemaName
    ->
      ^(TOK_DROP_TUPLE schemaName)
  ;

createViewStatement
@init {
msgs.push("create view statement");
}
@after {
msgs.pop();
}
  :
  CREATE VIEW schemaName AS FROM schemaName FILTER condition commentExpr?
    ->
      ^(TOK_CREATE_VIEW schemaName schemaName condition commentExpr?)
  ;

descViewStatement
@init {
msgs.push("describe view statement");
}
@after {
msgs.pop();
}
  :
  DESC VIEW schemaName
    ->
      ^(TOK_DESC_VIEW schemaName)
  ;

showViewsStatement
@init {
msgs.push("show views statement");
}
@after {
msgs.pop();
}
  :
  SHOW VIEWS
    -> TOK_SHOW_VIEWS
  ;

dropViewStatement
@init {
msgs.push("drop view statement");
}
@after {
msgs.pop();
}
  :
  DROP VIEW schemaName
    ->
      ^(TOK_DROP_VIEW schemaName)
  ;

schemaName
  :
  identifier
  ;

createFunctionStatement
@init {
msgs.push("create function statement");
}
@after {
msgs.pop();
}
  :
  CREATE FUNCTION functionName AS functionLocation commentExpr?
    ->
      ^(TOK_CREATE_FUNCTION functionName functionLocation commentExpr?)
  ;

descFunctionStatement
@init {
msgs.push("describe function statement");
}
@after {
msgs.pop();
}
  :
  DESC FUNCTION functionName
    ->
      ^(TOK_DESC_FUNCTION functionName)
  ;

showFunctionsStatement
@init {
msgs.push("show functions statement");
}
@after {
msgs.pop();
}
  :
  SHOW FUNCTIONS
    -> TOK_SHOW_FUNCTIONS
  ;

dropFunctionStatement
@init {
msgs.push("drop function statement");
}
@after {
msgs.pop();
}
  :
  DROP FUNCTION functionName
    ->
      ^(TOK_DROP_FUNCTION functionName)
  ;

functionLocation
  :
  StringLiteral
  ;

createUserStatement
@init {
msgs.push("create user statement");
}
@after {
msgs.pop();
}
  :
  CREATE USER userName IDENTIFIED BY password
    ->
      ^(TOK_CREATE_USER userName password)
  ;

descUserStatement
@init {
msgs.push("describe user statement");
}
@after {
msgs.pop();
}
  :
  DESC USER
    -> TOK_DESC_USER
  ;

showUsersStatement
@init {
msgs.push("show users statement");
}
@after {
msgs.pop();
}
  :
  SHOW USERS
    -> TOK_SHOW_USERS
  ;

alterUserStatement
@init {
msgs.push("alter user statement");
}
@after {
msgs.pop();
}
  :
  ALTER USER userName IDENTIFIED BY password
    ->
      ^(TOK_ALTER_USER userName password)
  ;

dropUserStatement
@init {
msgs.push("drop user statement");
}
@after {
msgs.pop();
}
  :
  DROP USER userName
    ->
      ^(TOK_DROP_USER userName)
  ;

userName
  :
  StringLiteral
  | identifier
  ;

password
  :
  StringLiteral
  ;

tclStatement
@init {
msgs.push("TCL statement");
}
@after {
msgs.pop();
}
  :
  | submitTopologyStatement
  | descTopologyStatement
  | showTopologiesStatement
  | stopTopologyStatement
  | startTopologyStatement
  | dropTopologyStatement
  | statsTopologyStatement
  | descClusterStatement
  ;

submitTopologyStatement
@init {
msgs.push("submit topology statement");
}
@after {
msgs.pop();
}
  :
  SUBMIT TOPOLOGY topologyName commentExpr?
    ->
      ^(TOK_SUBMIT_TOPOLOGY topologyName commentExpr?)
  ;

descTopologyStatement
@init {
msgs.push("describe topology statement");
}
@after {
msgs.pop();
}
  :
  DESC TOPOLOGY topologyName?
    ->
      ^(TOK_DESC_TOPOLOGY topologyName?)
  ;

showTopologiesStatement
@init {
msgs.push("show topologies statement");
}
@after {
msgs.pop();
}
  :
  SHOW TOPOLOGIES
    -> TOK_SHOW_TOPOLOGIES
  ;

stopTopologyStatement
@init {
msgs.push("stop topology statement");
}
@after {
msgs.pop();
}
  :
  STOP TOPOLOGY topologyName
    ->
      ^(TOK_STOP_TOPOLOGY topologyName)
  ;

startTopologyStatement
@init {
msgs.push("start topology statement");
}
@after {
msgs.pop();
}
  :
  START TOPOLOGY topologyName
    ->
      ^(TOK_START_TOPOLOGY topologyName)
  ;

dropTopologyStatement
@init {
msgs.push("drop topology statement");
}
@after {
msgs.pop();
}
  :
  DROP TOPOLOGY topologyName
    ->
      ^(TOK_DROP_TOPOLOGY topologyName)
  ;

statsTopologyStatement
@init {
msgs.push("stats topology statement");
}
@after {
msgs.pop();
}
  :
  STATS TOPOLOGY extended? topologyName?
    ->
      ^(TOK_STATS_TOPOLOGY extended? topologyName?)
  ;

topologyName
  :
  identifier
  ;

descClusterStatement
@init {
msgs.push("describe cluster statement");
}
@after {
msgs.pop();
}
  :
  DESC CLUSTER
    -> TOK_DESC_CLUSTER
  ;

constant
  :
  StringLiteral
  | number
  | bool
  ;

number
  :
  TinyintLiteral
  | SmallintLiteral
  | IntLiteral
  | BigintLiteral
  | FloatLiteral
  | DoubleLiteral
  ;

bool
  :
  TRUE
  | FALSE
  ;

field
  :
  fieldPart (DOT fieldPart)*
    ->
      ^(TOK_FIELD fieldPart+)
  ;

fieldPart
  :
  (
    identifier
    | ASTERISK
  )
  subscripts?
  ;

subscripts
  :
  subscript+
    ->
      ^(TOK_SUBSCRIPT subscript+)
  ;

subscript
  :
  LSQUARE!
  (
    StringLiteral
    | IntLiteral
    | arith
  )
  RSQUARE!
  ;

array
  :
  LSQUARE element (COMMA element)* RSQUARE
    ->
      ^(TOK_ARRAY element+)
  ;

element
  :
  (
    constant
    | field
  )
  ;

map
  :
  LCURLY constant COLON constant (COMMA constant COLON constant)* RCURLY
    ->
      ^(TOK_MAP constant+)
  ;

condition
  :
  orCondition
    ->
      ^(TOK_CONDITION orCondition)
  ;

orCondition
  :
  andCondition (or=OR andCondition)*
    -> {$or != null}?
      ^(TOK_OP_OR andCondition+)
    -> andCondition
  ;

andCondition
  :
  notCondition (and=AND notCondition)*
    -> {$and != null}?
      ^(TOK_OP_AND notCondition+)
    -> notCondition
  ;

notCondition
  :
  (not=NOT)* signCondition
    -> {$not != null}?
      ^(TOK_OP_NOT signCondition)
    -> signCondition
  ;

signCondition
  :
  leftExpr
  (
    (signOperator^ rightExpr)
    | (inOperator^ LPAREN! constant (COMMA! constant)+ RPAREN!)
    | (allOperator^ LPAREN! constant (COMMA! constant)+ RPAREN!)
    | (betweenOperator^ constant AND! constant)
    | isNullOperator^
  )+
  ;

leftExpr
  :
  field
  | function
  | (LPAREN! orCondition RPAREN!)
  ;

rightExpr
  :
  constant
  | field
  ;

signOperator
  :
  EQUAL
    -> TOK_OP_EQ
  | NOT_EQUAL
    -> TOK_OP_NE
  | LESS_OR_EQUAL
    -> TOK_OP_LE
  | LESS
    -> TOK_OP_LT
  | GREATER_OR_EQUAL
    -> TOK_OP_GE
  | GREATER
    -> TOK_OP_GT
  | LIKE
    -> TOK_OP_LIKE
  | REGEXP
    -> TOK_OP_REGEXP
  ;

inOperator
  :
  IN
    ->
      ^(TOK_OP_IN)
  ;

allOperator
  :
  ALL
    ->
      ^(TOK_OP_ALL)
  ;

betweenOperator
  :
  BETWEEN
    ->
      ^(TOK_OP_BETWEEN)
  ;

isNullOperator
  :
  (IS NULL)
    ->
      ^(TOK_OP_IS_NULL)
  | (IS NOT NULL)
    ->
      ^(TOK_OP_IS_NOT_NULL)
  ;

period
  :
  SecondsLiteral
  | MinutesLiteral
  | HoursLiteral
  | DaysLiteral
  ;

processor
  :
  processorName arguments
    ->
      ^(TOK_PROCESSOR processorName arguments)
  ;

processorName
  :
  Identifier
  ;

function
  :
  functionName arguments
    ->
      ^(TOK_FUNCTION functionName arguments)
  ;

functionName
  :
  identifier
  ;

arguments
  :
  LPAREN argument? (COMMA argument)* RPAREN
    ->
      ^(TOK_ARGUMENTS argument*)
  ;

argument
  :
  constant
  | arith
  | array
  | map
  | condition
  | period
  | cast
  | distinct
  ;

arith
  :
  arithMulExpr (arithAddOperator^ arithMulExpr)*
  ;

arithAddOperator
  :
  PLUS
  | MINUS
  ;

arithMulExpr
  :
  arithValue (arithMulOperator^ arithValue)*
  ;

arithMulOperator
  :
  ASTERISK
  | SLASH
  | PERCENT
  | DIV
  | MOD
  ;

arithValue
  :
  field
  | function
  | number
  | (LPAREN! arith RPAREN!)
  ;

distinct
  :
  DISTINCT
  (
    field
    | function
    | cast
  )
    ->
      ^(TOK_DISTINCT function? field? cast?)
  ;

countInterval
  :
  IntLiteral
    ->
      ^(TOK_COUNT_INTERVAL IntLiteral)
  ;

timeInterval
  :
  period
    ->
      ^(TOK_TIME_INTERVAL period)
  ;

cronInterval
  :
  cronExpr
    ->
      ^(TOK_CRON_INTERVAL cronExpr)
  ;

cronExpr
  :
  StringLiteral
  ;

parallelism
  :
  PARALLELISM IntLiteral
    ->
      ^(TOK_PARALLELISM IntLiteral)
  ;

commentExpr
  :
  COMMENT comment
    ->
      ^(TOK_COMMENT comment)
  ;

comment
  :
  StringLiteral
  ;

identifier
  :
  Identifier
  | nonReserved
    -> Identifier[$nonReserved.text]
  ;

nonReserved
  :
  FROM
  | JOIN
  | ON
  | EMIT
  | TO
  | INTO
  | EACH
  | FILTER
  | EXPIRE
  | STATE
  | SLIDE
  | LENGTH
  | SNAPSHOT
  | EVERY
  | LIMIT
  | FIRST
  | LAST
  | GROUP
  | BEGIN
  | END
  | PARTITION
  | BY
  | STREAM
  | AS
  | USING
  | PARALLELISM
  | EXPLAIN
  | EXTENDED
  | CLEAR
  | CREATE
  | TUPLE
  | VIEW
  | PARTITIONED
  | COMMENT
  | DESC
  | SHOW
  | TUPLES
  | VIEWS
  | ALTER
  | DROP
  | FUNCTION
  | FUNCTIONS
  | USER
  | IDENTIFIED
  | USERS
  | STRING
  | TINYINT
  | SMALLINT
  | INT
  | BIGINT
  | FLOAT
  | DOUBLE
  | BOOLEAN
  | TIMESTAMP
  | LIST
  | MAP
  | STRUCT
  | TOPOLOGY
  | SUBMIT
  | STOP
  | START
  | TOPOLOGIES
  | CLUSTER
  | TRUE
  | FALSE
  | LIKE
  | REGEXP
  | IN
  | ALL
  | BETWEEN
  | IS
  ;

FROM
  :
  'FROM'
  ;

JOIN
  :
  'JOIN'
  ;

ON
  :
  'ON'
  ;

EMIT
  :
  'EMIT'
  ;

TO
  :
  'TO'
  ;

INTO
  :
  'INTO'
  ;

EACH
  :
  'EACH'
  ;

CAST
  :
  'CAST'
  ;

DISTINCT
  :
  'DISTINCT'
  ;

FILTER
  :
  'FILTER'
  ;

EXPIRE
  :
  'EXPIRE'
  ;

STATE
  :
  'STATE'
  ;

SLIDE
  :
  'SLIDE'
  ;

LENGTH
  :
  'LENGTH'
  ;

SNAPSHOT
  :
  'SNAPSHOT'
  ;

EVERY
  :
  'EVERY'
  ;

LIMIT
  :
  'LIMIT'
  ;

FIRST
  :
  'FIRST'
  ;

LAST
  :
  'LAST'
  ;

GROUP
  :
  'GROUP'
  ;

BEGIN
  :
  'BEGIN'
  ;

END
  :
  'END'
  ;

PARTITION
  :
  'PARTITION'
  ;

BY
  :
  'BY'
  ;

STREAM
  :
  'STREAM'
  ;

AS
  :
  'AS'
  ;

USING
  :
  'USING'
  ;

PARALLELISM
  :
  'PARALLELISM'
  ;

EXPLAIN
  :
  'EXPLAIN'
  ;

EXTENDED
  :
  'EXTENDED'
  ;

CLEAR
  :
  'CLEAR'
  ;

CREATE
  :
  'CREATE'
  ;

TUPLE
  :
  'TUPLE'
  ;

VIEW
  :
  'VIEW'
  ;

PARTITIONED
  :
  'PARTITIONED'
  ;

COMMENT
  :
  'COMMENT'
  ;

DESC
  :
  'DESCRIBE'
  | 'DESC'
  ;

SHOW
  :
  'SHOW'
  ;

TUPLES
  :
  'TUPLES'
  ;

VIEWS
  :
  'VIEWS'
  ;

FUNCTION
  :
  'FUNCTION'
  ;

FUNCTIONS
  :
  'FUNCTIONS'
  ;

ALTER
  :
  'ALTER'
  ;

DROP
  :
  'DROP'
  ;

USER
  :
  'USER'
  ;

IDENTIFIED
  :
  'IDENTIFIED'
  ;

USERS
  :
  'USERS'
  ;

STRING
  :
  'STRING'
  ;

TINYINT
  :
  'TINYINT'
  ;

SMALLINT
  :
  'SMALLINT'
  ;

INT
  :
  'INT'
  ;

BIGINT
  :
  'BIGINT'
  ;

FLOAT
  :
  'FLOAT'
  ;

DOUBLE
  :
  'DOUBLE'
  ;

BOOLEAN
  :
  'BOOLEAN'
  ;

TIMESTAMP
  :
  'TIMESTAMP'
  ;

LIST
  :
  'LIST'
  ;

MAP
  :
  'MAP'
  ;

STRUCT
  :
  'STRUCT'
  ;

TOPOLOGY
  :
  'TOPOLOGY'
  ;

SUBMIT
  :
  'SUBMIT'
  ;

STOP
  :
  'STOP'
  ;

START
  :
  'START'
  ;

STATS
  :
  'STATS'
  ;

TOPOLOGIES
  :
  'TOPOLOGIES'
  ;

CLUSTER
  :
  'CLUSTER'
  ;

TRUE
  :
  'TRUE'
  ;

FALSE
  :
  'FALSE'
  ;

LIKE
  :
  'LIKE'
  ;

REGEXP
  :
  'REGEXP'
  ;

IN
  :
  'IN'
  ;

ALL
  :
  'ALL'
  ;

BETWEEN
  :
  'BETWEEN'
  ;

IS
  :
  'IS'
  ;

NULL
  :
  'NULL'
  ;

AND
  :
  'AND'
  ;

OR
  :
  'OR'
  ;

NOT
  :
  'NOT'
  | '!'
  ;

DOT
  :
  '.'
  ;

COLON
  :
  ':'
  ;

COMMA
  :
  ','
  ;

SEMICOLON
  :
  ';'
  ;

LPAREN
  :
  '('
  ;

RPAREN
  :
  ')'
  ;

LSQUARE
  :
  '['
  ;

RSQUARE
  :
  ']'
  ;

LCURLY
  :
  '{'
  ;

RCURLY
  :
  '}'
  ;

EQUAL
  :
  '='
  | '=='
  ;

NOT_EQUAL
  :
  '<>'
  | '!='
  ;

LESS_OR_EQUAL
  :
  '<='
  ;

LESS
  :
  '<'
  ;

GREATER_OR_EQUAL
  :
  '>='
  ;

GREATER
  :
  '>'
  ;

PLUS
  :
  '+'
  ;

MINUS
  :
  '-'
  ;

ASTERISK
  :
  '*'
  ;

SLASH
  :
  '/'
  ;

PERCENT
  :
  '%'
  ;

DIV
  :
  'DIV'
  ;

MOD
  :
  'MOD'
  ;

AMPERSAND
  :
  '&'
  ;

TILDE
  :
  '~'
  ;

BITWISEOR
  :
  '|'
  ;

BITWISEXOR
  :
  '^'
  ;

QUESTION
  :
  '?'
  ;

DOLLAR
  :
  '$'
  ;

fragment
Letter
  :
  'a'..'z'
  | 'A'..'Z'
  ;

fragment
Digit
  :
  '0'..'9'
  ;

fragment
QuotedIdentifier
  :
  '`'  ( '``' | ~('`') )* '`' { setText(getText().substring(1, getText().length() -1 ).replaceAll("``", "`")); }
  ;

StringLiteral
  :
  (
    '\''
    (
      ~(
        '\''
        | '\\'
       )
      | ('\\' .)
    )*
    '\''
    | '\"'
    (
      ~(
        '\"'
        | '\\'
       )
      | ('\\' .)
    )*
    '\"'
  )+
  ;

TinyintLiteral
  :
  MINUS? (Digit)+ 'Y'
  ;

SmallintLiteral
  :
  MINUS? (Digit)+ 'S'
  ;

IntLiteral
  :
  MINUS? (Digit)+
  ;

BigintLiteral
  :
  MINUS? (Digit)+ 'L'
  ;

FloatLiteral
  :
  MINUS? (Digit)+ DOT (Digit)+ 'F'
  ;

DoubleLiteral
  :
  MINUS? (Digit)+ DOT (Digit)+
  ;

SecondsLiteral
  :
  (Digit)+
  (
    'SECONDS'
    | 'SEC'
  )
  ;

MinutesLiteral
  :
  (Digit)+
  (
    'MINUTES'
    | 'MIN'
  )
  ;

HoursLiteral
  :
  (Digit)+
  (
    'HOURS'
    | 'H'
  )
  ;

DaysLiteral
  :
  (Digit)+
  (
    'DAYS'
    | 'D'
  )
  ;

Identifier
  :
  (
    Letter
    | Digit
    | '_'
  )+
  ;

WS
  :
  (
    ' '
    | '\r'
    | '\t'
    | '\n'
  )

  {
   $channel = HIDDEN;
  }
  ;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar SqlBase;

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

standaloneExpression
    : expression EOF
    ;

standaloneRoutineBody
    : routineBody EOF
    ;

statement
    : query                                                            #statementDefault
    | USE schema=identifier                                            #use
    | USE catalog=identifier DOT schema=identifier                     #use
    | CREATE SCHEMA (IF NOT EXISTS)? qualifiedName
        (WITH properties)?                                             #createSchema
    | DROP SCHEMA (IF EXISTS)? qualifiedName (CASCADE | RESTRICT)?     #dropSchema
    | ALTER SCHEMA qualifiedName RENAME TO identifier                  #renameSchema
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName columnAliases?
        (COMMENT string)?
        (WITH properties)? AS (query | LPAREN query RPAREN )
        (WITH (NO)? DATA)?                                             #createTableAsSelect
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName
        LPAREN  tableElement (COMMA  tableElement)* RPAREN
         (COMMENT string)?
         (WITH properties)?                                            #createTable
    | DROP TABLE (IF EXISTS)? qualifiedName                            #dropTable
    | INSERT INTO qualifiedName columnAliases? query                   #insertInto
    | DELETE FROM qualifiedName (WHERE booleanExpression)?             #delete
    | ALTER TABLE (IF EXISTS)? from=qualifiedName
        RENAME TO to=qualifiedName                                     #renameTable
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        RENAME COLUMN (IF EXISTS)? from=identifier TO to=identifier    #renameColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        DROP COLUMN (IF EXISTS)? column=qualifiedName                  #dropColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        DROP INDEX (IF EXISTS)? index=qualifiedName                    #dropIndex
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        ADD COLUMN (IF NOT EXISTS)? column=columnDefinition            #addColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        ADD INDEX (IF NOT EXISTS)? index=indexDefinition               #addIndex
    | ANALYZE qualifiedName (WITH properties)?                         #analyze
    | CREATE (OR REPLACE)? VIEW qualifiedName
            (SECURITY (DEFINER | INVOKER))? AS query                   #createView
    | DROP VIEW (IF EXISTS)? qualifiedName                             #dropView
    | CREATE MATERIALIZED VIEW (IF NOT EXISTS)? qualifiedName
        (COMMENT string)?
        (WITH properties)? AS (query | LPAREN query RPAREN )                    #createMaterializedView
    | DROP MATERIALIZED VIEW (IF EXISTS)? qualifiedName                #dropMaterializedView
    | CREATE (OR REPLACE)? TEMPORARY? FUNCTION functionName=qualifiedName
        LPAREN  (sqlParameterDeclaration (COMMA  sqlParameterDeclaration)*)? RPAREN
        RETURNS returnType=type
        (COMMENT string)?
        routineCharacteristics routineBody                             #createFunction
    | ALTER FUNCTION qualifiedName types?
      alterRoutineCharacteristics                                      #alterFunction
    | DROP TEMPORARY? FUNCTION (IF EXISTS)? qualifiedName types?       #dropFunction
    | CALL qualifiedName LPAREN  (callArgument (COMMA  callArgument)*)? RPAREN    #call
    | CREATE ROLE name=identifier
        (WITH ADMIN grantor)?                                          #createRole
    | DROP ROLE name=identifier                                        #dropRole
    | GRANT
        roles
        TO principal (COMMA  principal)*
        (WITH ADMIN OPTION)?
        (GRANTED BY grantor)?                                          #grantRoles
    | REVOKE
        (ADMIN OPTION FOR)?
        roles
        FROM principal (COMMA  principal)*
        (GRANTED BY grantor)?                                          #revokeRoles
    | SET ROLE (ALL | NONE | role=identifier)                          #setRole
    | GRANT
        (privilege (COMMA  privilege)* | ALL PRIVILEGES)
        ON TABLE? qualifiedName TO grantee=principal
        (WITH GRANT OPTION)?                                           #grant
    | REVOKE
        (GRANT OPTION FOR)?
        (privilege (COMMA  privilege)* | ALL PRIVILEGES)
        ON TABLE? qualifiedName FROM grantee=principal                #revoke
    | SHOW GRANTS
        (ON TABLE? qualifiedName)?                                     #showGrants
    | EXPLAIN ANALYZE? VERBOSE?
        (LPAREN  explainOption (COMMA  explainOption)* RPAREN )? statement        #explain
    | SHOW CREATE TABLE qualifiedName                                  #showCreateTable
    | SHOW CREATE VIEW qualifiedName                                   #showCreateView
    | SHOW CREATE MATERIALIZED VIEW qualifiedName                      #showCreateMaterializedView
    | SHOW CREATE FUNCTION qualifiedName types?                        #showCreateFunction
    | SHOW TABLES ((FROM | IN) qualifiedName)?
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showTables
    | SHOW SCHEMAS ((FROM | IN) identifier)?
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showSchemas
    | SHOW CATALOGS (LIKE pattern=string)?                             #showCatalogs
    | SHOW COLUMNS (FROM | IN) qualifiedName                           #showColumns
    | SHOW STATS FOR qualifiedName                                     #showStats
    | SHOW STATS FOR LPAREN  querySpecification RPAREN                         #showStatsForQuery
    | SHOW CURRENT? ROLES ((FROM | IN) identifier)?                    #showRoles
    | SHOW ROLE GRANTS ((FROM | IN) identifier)?                       #showRoleGrants
    | DESCRIBE qualifiedName                                           #showColumns
    | DESC qualifiedName                                               #showColumns
    | SHOW FUNCTIONS
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showFunctions
    | SHOW SESSION                                                     #showSession
    | SET SESSION qualifiedName EQ expression                          #setSession
    | RESET SESSION qualifiedName                                      #resetSession
    | START TRANSACTION (transactionMode (COMMA  transactionMode)*)?      #startTransaction
    | COMMIT WORK?                                                     #commit
    | ROLLBACK WORK?                                                   #rollback
    | PREPARE identifier FROM statement                                #prepare
    | DEALLOCATE PREPARE identifier                                    #deallocate
    | EXECUTE identifier (USING expression (COMMA  expression)*)?         #execute
    | DESCRIBE INPUT identifier                                        #describeInput
    | DESCRIBE OUTPUT identifier                                       #describeOutput
    ;

query
    :  with? queryNoWith
    ;

with
    : WITH RECURSIVE? namedQuery (COMMA  namedQuery)*
    ;

tableElement
    : columnDefinition
    | likeClause
    | indexClause
    ;

indexClause
    : INDEX indexDefinition
    ;

columnDefinition
    : identifier type (NOT NULL)? (DEFAULT defauleValue=expression)? (COMMENT string)? (WITH properties)?
    ;

indexDefinition
    : identifier columnAliases (USING type)?
    ;

likeClause
    : LIKE qualifiedName (optionType=(INCLUDING | EXCLUDING) PROPERTIES)?
    ;

properties
    : LPAREN  property (COMMA  property)* RPAREN
    ;

property
    : identifier EQ expression
    ;

sqlParameterDeclaration
    : identifier type
    ;

routineCharacteristics
    : routineCharacteristic*
    ;

routineCharacteristic
    : LANGUAGE language
    | determinism
    | nullCallClause
    ;

alterRoutineCharacteristics
    : alterRoutineCharacteristic*
    ;

alterRoutineCharacteristic
    : nullCallClause
    ;

routineBody
    : returnStatement
    | externalBodyReference
    ;

returnStatement
    : RETURN expression
    ;

externalBodyReference
    : EXTERNAL (NAME externalRoutineName)?
    ;

language
    : SQL
    | identifier
    ;

determinism
    : DETERMINISTIC
    | NOT DETERMINISTIC;

nullCallClause
    : RETURNS NULL ON NULL INPUT
    | CALLED ON NULL INPUT
    ;

externalRoutineName
    : identifier
    ;

queryNoWith:
      queryTerm
      (ORDER BY sortItem (COMMA  sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | TABLE qualifiedName                  #table
    | VALUES expression (COMMA  expression)*  #inlineTable
    | LPAREN  queryNoWith  RPAREN                  #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (COMMA  selectItem)*
      (FROM relation (COMMA  relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
    ;

groupBy
    : setQuantifier? groupingElement (COMMA  groupingElement)*
    ;

groupingElement
    : groupingSet                                            #singleGroupingSet
    | ROLLUP LPAREN  (expression (COMMA  expression)*)? RPAREN          #rollup
    | CUBE LPAREN  (expression (COMMA  expression)*)? RPAREN            #cube
    | GROUPING SETS LPAREN  groupingSet (COMMA  groupingSet)* RPAREN    #multipleGroupingSets
    ;

groupingSet
    : LPAREN  (expression (COMMA  expression)*)? RPAREN
    | expression
    ;

namedQuery
    : name=identifier (columnAliases)? AS LPAREN  query RPAREN 
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | qualifiedName DOT ASTERISK    #selectAll
    | ASTERISK                      #selectAll
    ;

relation
    : left=relation
      (CROSS JOIN right=sampledRelation
      | joinType JOIN rightRelation=relation joinCriteria
      | NATURAL joinType JOIN right=sampledRelation
      )                                           #joinRelation
    | sampledRelation                             #relationDefault
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING LPAREN  identifier (COMMA  identifier)* RPAREN
    ;

sampledRelation
    : aliasedRelation (
        TABLESAMPLE sampleType LPAREN  percentage=expression RPAREN 
      )?
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : LPAREN  identifier (COMMA  identifier)* RPAREN
    ;

relationPrimary
    : qualifiedName                                                   #tableName
    | LPAREN  query RPAREN                                                    #subqueryRelation
    | UNNEST LPAREN  expression (COMMA  expression)* RPAREN  (WITH ORDINALITY)?  #unnest
    | LATERAL LPAREN  query RPAREN                                            #lateral
    | LPAREN  relation RPAREN                                                 #parenthesizedRelation
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : valueExpression predicate[$valueExpression.ctx]?             #predicated
    | NOT booleanExpression                                        #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

// workaround for https://github.com/antlr/antlr4/issues/780
predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | comparisonOperator comparisonQuantifier LPAREN  query RPAREN                #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN LPAREN  expression (COMMA  expression)* RPAREN                         #inList
    | NOT? IN LPAREN  query RPAREN                                                #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | valueExpression AT timeZoneSpecifier                                              #atTimeZone
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    ;

primaryExpression
    : NULL                                                                                #nullLiteral
    | interval                                                                            #intervalLiteral
    | identifier string                                                                   #typeConstructor
    | DOUBLE_PRECISION string                                                             #typeConstructor
    | number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | string                                                                              #stringLiteral
    | BINARY_LITERAL                                                                      #binaryLiteral
    |  QUESTION                                                                                   #parameter
    | POSITION LPAREN  valueExpression IN valueExpression RPAREN                                  #position
    | LPAREN  expression (COMMA  expression)+ RPAREN                                                 #rowConstructor
    | ROW LPAREN  expression (COMMA  expression)* RPAREN                                             #rowConstructor
    | qualifiedName LPAREN  ASTERISK RPAREN  filter? over?                                        #functionCall
    | qualifiedName LPAREN  (setQuantifier? expression (COMMA  expression)*)?
        (ORDER BY sortItem (COMMA  sortItem)*)? RPAREN  filter? (nullTreatment? over)?           #functionCall
    | identifier  ARROW   expression                                                          #lambda
    | LPAREN  (identifier (COMMA  identifier)*)? RPAREN   ARROW   expression                             #lambda
    | LPAREN  query RPAREN                                                                        #subqueryExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS LPAREN  query RPAREN                                                                 #exists
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END              #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | CAST LPAREN  expression AS type RPAREN                                                      #cast
    | TRY_CAST LPAREN  expression AS type RPAREN                                                  #cast
    | ARRAY  LBRACK   (expression (COMMA  expression)*)?  RBRACK                                         #arrayConstructor
    | value=primaryExpression  LBRACK   index=valueExpression  RBRACK                                 #subscript
    | identifier                                                                          #columnReference
    | base=primaryExpression DOT fieldName=identifier                                     #dereference
    | name=CURRENT_DATE                                                                   #specialDateTimeFunction
    | name=CURRENT_TIME (LPAREN  precision=INTEGER_VALUE RPAREN )?                                #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP (LPAREN  precision=INTEGER_VALUE RPAREN )?                           #specialDateTimeFunction
    | name=LOCALTIME (LPAREN  precision=INTEGER_VALUE RPAREN )?                                   #specialDateTimeFunction
    | name=LOCALTIMESTAMP (LPAREN  precision=INTEGER_VALUE RPAREN )?                              #specialDateTimeFunction
    | name=CURRENT_USER                                                                   #currentUser
    | SUBSTRING LPAREN  valueExpression FROM valueExpression (FOR valueExpression)? RPAREN        #substring
    | NORMALIZE LPAREN  valueExpression (COMMA  normalForm)? RPAREN                                  #normalize
    | EXTRACT LPAREN  identifier FROM valueExpression RPAREN                                      #extract
    | LPAREN  expression RPAREN                                                                   #parenthesizedExpression
    | GROUPING LPAREN  (qualifiedName (COMMA  qualifiedName)*)? RPAREN                               #groupingOperation
    ;

string
    : STRING                                #basicStringLiteral
    | UNICODE_STRING (UESCAPE STRING)?      #unicodeStringLiteral
    ;

nullTreatment
    : IGNORE NULLS
    | RESPECT NULLS
    ;

timeZoneSpecifier
    : TIME ZONE interval  #timeZoneInterval
    | TIME ZONE string    #timeZoneString
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

comparisonQuantifier
    : ALL | SOME | ANY
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? string from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

normalForm
    : NFD | NFC | NFKD | NFKC
    ;

types
    : LPAREN  (type (COMMA  type)*)? RPAREN
    ;

type
    : type ARRAY
    | ARRAY LT type GT
    | MAP LT type COMMA  type GT
    | ROW LPAREN  identifier type (COMMA  identifier type)* RPAREN
    | baseType (LPAREN  typeParameter (COMMA  typeParameter)* RPAREN )?
    | INTERVAL from=intervalField TO to=intervalField
    ;

typeParameter
    : INTEGER_VALUE | type
    ;

baseType
    : TIME_WITH_TIME_ZONE
    | TIMESTAMP_WITH_TIME_ZONE
    | DOUBLE_PRECISION
    | qualifiedName
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

filter
    : FILTER LPAREN  WHERE booleanExpression RPAREN 
    ;

over
    : OVER LPAREN 
        (PARTITION BY partition+=expression (COMMA  partition+=expression)*)?
        (ORDER BY sortItem (COMMA  sortItem)*)?
        windowFrame?
      RPAREN 
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame // expression should be unsignedLiteral
    ;


explainOption
    : FORMAT value=(TEXT | GRAPHVIZ | JSON)                 #explainFormat
    | TYPE value=(LOGICAL | DISTRIBUTED | VALIDATE | IO)    #explainType
    ;

transactionMode
    : ISOLATION LEVEL levelOfIsolation    #isolationLevel
    | READ accessMode=(ONLY | WRITE)      #transactionAccessMode
    ;

levelOfIsolation
    : READ UNCOMMITTED                    #readUncommitted
    | READ COMMITTED                      #readCommitted
    | REPEATABLE READ                     #repeatableRead
    | SERIALIZABLE                        #serializable
    ;

callArgument
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    ;

privilege
    : SELECT | DELETE | INSERT | identifier
    ;

qualifiedName
    : identifier (DOT identifier)*
    ;

grantor
    : CURRENT_USER          #currentUserGrantor
    | CURRENT_ROLE          #currentRoleGrantor
    | principal             #specifiedPrincipal
    ;

principal
    : USER identifier       #userPrincipal
    | ROLE identifier       #rolePrincipal
    | identifier            #unspecifiedPrincipal
    ;

roles
    : identifier (COMMA  identifier)*
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | QUOTED_IDENTIFIER      #quotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

number
    : DECIMAL_VALUE  #decimalLiteral
    | DOUBLE_VALUE   #doubleLiteral
    | INTEGER_VALUE  #integerLiteral
    ;

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    : ADD | ADMIN | ALL | ANALYZE | ANY | ARRAY | ASC | AT
    | BERNOULLI
    | CALL | CALLED | CASCADE | CATALOGS | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | CURRENT | CURRENT_ROLE
    | DATA | DATE | DAY | DEFINER | DESC | DETERMINISTIC | DISTRIBUTED
    | EXCLUDING | EXPLAIN | EXTERNAL
    | FILTER | FIRST | FOLLOWING | FORMAT | FUNCTION | FUNCTIONS
    | GRANT | GRANTED | GRANTS | GRAPHVIZ
    | HOUR
    | IF | IGNORE | INCLUDING | INPUT | INTERVAL | INVOKER | IO | ISOLATION
    | JSON
    | LANGUAGE | LAST | LATERAL | LEVEL | LIMIT | LOGICAL
    | MAP | MATERIALIZED | MINUTE | MONTH
    | NAME | NFC | NFD | NFKC | NFKD | NO | NONE | NULLIF | NULLS
    | ONLY | OPTION | ORDINALITY | OUTPUT | OVER
    | PARTITION | PARTITIONS | POSITION | PRECEDING | PRIVILEGES | PROPERTIES
    | RANGE | READ | RENAME | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | RETURN | RETURNS | REVOKE | ROLE | ROLES | ROLLBACK | ROW | ROWS
    | SCHEMA | SCHEMAS | SECOND | SECURITY | SERIALIZABLE | SESSION | SET | SETS | SQL
    | SHOW | SOME | START | STATS | SUBSTRING | SYSTEM
    | TABLES | TABLESAMPLE | TEMPORARY | TEXT | TIME | TIMESTAMP | TO | TRANSACTION | TRY_CAST | TYPE
    | UNBOUNDED | UNCOMMITTED | USE | USER
    | VALIDATE | VERBOSE | VIEW
    | WORK | WRITE
    | YEAR
    | ZONE
    ;

ADD: 'ADD';
ADMIN: 'ADMIN';
ALL: 'ALL';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
AND: 'AND';
ANY: 'ANY';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
AT: 'AT';
BERNOULLI: 'BERNOULLI';
BETWEEN: 'BETWEEN';
BY: 'BY';
CALL: 'CALL';
CALLED: 'CALLED';
CASCADE: 'CASCADE';
CASE: 'CASE';
CAST: 'CAST';
CATALOGS: 'CATALOGS';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMMITTED: 'COMMITTED';
CONSTRAINT: 'CONSTRAINT';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_ROLE: 'CURRENT_ROLE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DATA: 'DATA';
DATE: 'DATE';
DAY: 'DAY';
DEALLOCATE: 'DEALLOCATE';
DEFINER: 'DEFINER';
DELETE: 'DELETE';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DETERMINISTIC: 'DETERMINISTIC';
DISTINCT: 'DISTINCT';
DISTRIBUTED: 'DISTRIBUTED';
DROP: 'DROP';
ELSE: 'ELSE';
END: 'END';
ESCAPE: 'ESCAPE';
EXCEPT: 'EXCEPT';
EXCLUDING: 'EXCLUDING';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXTRACT: 'EXTRACT';
EXTERNAL: 'EXTERNAL';
FALSE: 'FALSE';
FILTER: 'FILTER';
FIRST: 'FIRST';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FORMAT: 'FORMAT';
FROM: 'FROM';
FULL: 'FULL';
FUNCTION: 'FUNCTION';
FUNCTIONS: 'FUNCTIONS';
GRANT: 'GRANT';
GRANTED: 'GRANTED';
GRANTS: 'GRANTS';
GRAPHVIZ: 'GRAPHVIZ';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
HAVING: 'HAVING';
HOUR: 'HOUR';
IF: 'IF';
IGNORE: 'IGNORE';
IN: 'IN';
INCLUDING: 'INCLUDING';
INNER: 'INNER';
INPUT: 'INPUT';
INSERT: 'INSERT';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
INVOKER: 'INVOKER';
IO: 'IO';
IS: 'IS';
ISOLATION: 'ISOLATION';
JSON: 'JSON';
JOIN: 'JOIN';
LANGUAGE: 'LANGUAGE';
LAST: 'LAST';
LATERAL: 'LATERAL';
LEFT: 'LEFT';
LEVEL: 'LEVEL';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
LOGICAL: 'LOGICAL';
MAP: 'MAP';
MATERIALIZED: 'MATERIALIZED';
MINUTE: 'MINUTE';
MONTH: 'MONTH';
NAME: 'NAME';
NATURAL: 'NATURAL';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO';
NONE: 'NONE';
NORMALIZE: 'NORMALIZE';
NOT: 'NOT';
NULL: 'NULL';
NULLIF: 'NULLIF';
NULLS: 'NULLS';
ON: 'ON';
ONLY: 'ONLY';
OPTION: 'OPTION';
OR: 'OR';
ORDER: 'ORDER';
ORDINALITY: 'ORDINALITY';
OUTER: 'OUTER';
OUTPUT: 'OUTPUT';
OVER: 'OVER';
PARTITION: 'PARTITION';
PARTITIONS: 'PARTITIONS';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PREPARE: 'PREPARE';
PRIVILEGES: 'PRIVILEGES';
PROPERTIES: 'PROPERTIES';
RANGE: 'RANGE';
READ: 'READ';
RECURSIVE: 'RECURSIVE';
RENAME: 'RENAME';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
RESET: 'RESET';
RESPECT: 'RESPECT';
RESTRICT: 'RESTRICT';
RETURN: 'RETURN';
RETURNS: 'RETURNS';
REVOKE: 'REVOKE';
RIGHT: 'RIGHT';
ROLE: 'ROLE';
ROLES: 'ROLES';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROW: 'ROW';
ROWS: 'ROWS';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECOND: 'SECOND';
SECURITY: 'SECURITY';
SELECT: 'SELECT';
SERIALIZABLE: 'SERIALIZABLE';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
SOME: 'SOME';
SQL: 'SQL';
START: 'START';
STATS: 'STATS';
SUBSTRING: 'SUBSTRING';
SYSTEM: 'SYSTEM';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TEMPORARY: 'TEMPORARY';
TEXT: 'TEXT';
THEN: 'THEN';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TRANSACTION: 'TRANSACTION';
TRUE: 'TRUE';
TRY_CAST: 'TRY_CAST';
TYPE: 'TYPE';
UESCAPE: 'UESCAPE';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNION: 'UNION';
UNNEST: 'UNNEST';
USE: 'USE';
USER: 'USER';
USING: 'USING';
VALIDATE: 'VALIDATE';
VALUES: 'VALUES';
VERBOSE: 'VERBOSE';
VIEW: 'VIEW';
WHEN: 'WHEN';
WHERE: 'WHERE';
WITH: 'WITH';
WORK: 'WORK';
WRITE: 'WRITE';
YEAR: 'YEAR';
ZONE: 'ZONE';
INDEX: 'INDEX';
DEFAULT: 'DEFAULT';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';

DOT : '.' ;
LPAREN : '('  ;
RPAREN : ')'  ;
COMMA : ','  ;
QUESTION : '?' ;
LBRACK : '[' ;
RBRACK : ']' ;
ARROW: '->';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    :  'X\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ DOT DIGIT*
    | DOT DIGIT+
    ;

DOUBLE_VALUE
    : DIGIT+ (DOT DIGIT*)? EXPONENT
    | DOT DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' | ':')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@' | ':')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

TIME_WITH_TIME_ZONE
    : 'TIME' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

TIMESTAMP_WITH_TIME_ZONE
    : 'TIMESTAMP' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

DOUBLE_PRECISION
    : 'DOUBLE' WS 'PRECISION'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;

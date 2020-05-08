/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.connectors.athena.jdbc.kdb;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Extends {@link JdbcSplitQueryBuilder} and implements Kdb specific SQL clauses for split.
 *
 * Kdb provides named partitions which can be used in a FROM clause.
 */
public class KdbQueryStringBuilder
        extends JdbcSplitQueryBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KdbQueryStringBuilder.class);
    private static final org.joda.time.LocalDateTime EPOCH = new org.joda.time.LocalDateTime(1970, 1, 1, 0, 0);

    public KdbQueryStringBuilder(final String quoteCharacters)
    {
        super(quoteCharacters);
    }

    /**
     * Common logic to build Split SQL including constraints translated in where clause.
     *
     * @param jdbcConnection JDBC connection. See {@link Connection}.
     * @param catalog Athena provided catalog name.
     * @param schema table schema name.
     * @param table table name.
     * @param tableSchema table schema (column and type information).
     * @param constraints constraints passed by Athena to push down.
     * @param split table split.
     * @return prepated statement with SQL. See {@link PreparedStatement}.
     * @throws SQLException JDBC database exception.
     */
    @Override
    public PreparedStatement buildSql(
            final Connection jdbcConnection,
            final String catalog,
            final String schema,
            final String table,
            final Schema tableSchema,
            final Constraints constraints,
            final Split split)
            throws SQLException
    {
        final String sql = buildSqlString(catalog, schema, table, tableSchema, constraints, split);
        PreparedStatement statement = jdbcConnection.prepareStatement(sql);

        return statement;
    }
      
    /**
     * Common logic to build Split SQL including constraints translated in where clause.
     *
     * @param catalog Athena provided catalog name.
     * @param schema table schema name.
     * @param table table name.
     * @param tableSchema table schema (column and type information).
     * @param constraints constraints passed by Athena to push down.
     * @param split table split.
     * @return prepated statement with SQL. See {@link PreparedStatement}.
     * @throws SQLException JDBC database exception.
     */
    @VisibleForTesting
    String buildSqlString(
            final String catalog,
            final String schema,
            final String table,
            final Schema tableSchema,
            final Constraints constraints,
            final Split split)
            throws SQLException
    {   
        StringBuilder sql = new StringBuilder();

        String columnNames = tableSchema.getFields().stream()
                .map(Field::getName)
                .filter(c -> !split.getProperties().containsKey(c))
                .map(this::quote)
                .collect(Collectors.joining(", "));

        sql.append("q) ");
        sql.append("select ");
        sql.append(columnNames);
        if (columnNames.isEmpty()) {
            sql.append("null");
        }

        sql.append(getFromClauseWithSplit(catalog, schema, table, split));

        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> clauses = toConjuncts(tableSchema.getFields(), constraints, accumulator, split.getProperties());
        clauses.addAll(getPartitionWhereClauses(split));
        if (!clauses.isEmpty()) {
            sql.append(" where ")
                    .append(Joiner.on(" , ").join(clauses));
        }

        LOGGER.info("Generated SQL : {}", sql.toString());

        return sql.toString();
    }

    @Override
    protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split)
    {
        StringBuilder tableName = new StringBuilder();
        // if (!Strings.isNullOrEmpty(catalog)) {
        //     tableName.append(quote(catalog)).append('.');
        // }
        // if (!Strings.isNullOrEmpty(schema)) {
        //     tableName.append(quote(schema)).append('.');
        // }
        tableName.append(quote(table));

        String partitionName = split.getProperty(KdbMetadataHandler.BLOCK_PARTITION_COLUMN_NAME);

        if (KdbMetadataHandler.ALL_PARTITIONS.equals(partitionName)) {
            // No partitions
            return String.format(" from %s ", tableName);
        }

        return String.format(" from %s PARTITION(%s) ", tableName, partitionName);
    }

    @Override
    protected List<String> getPartitionWhereClauses(final Split split)
    {
        return Collections.emptyList();
    }

    private static final ThreadLocal<DateTimeFormatter> DATE_FORMAT = new ThreadLocal<DateTimeFormatter>() {
        @Override
        protected DateTimeFormatter initialValue() {
            return DateTimeFormat.forPattern("yyyy.MM.dd");
        }
    };

    private static final ThreadLocal<DateTimeFormatter> TIME_FORMAT = new ThreadLocal<DateTimeFormatter>() {
        @Override
        protected DateTimeFormatter initialValue() {
            return DateTimeFormat.forPattern("HH:mm:ss.SSSSSS000");
        }
    };

    static String toLiteral(Object value, ArrowType type, String columnName, Field column) {        
        LOGGER.info("column:" + String.valueOf(columnName) + " value:" + String.valueOf(value));
        String literal = toLiteral(value, type, Types.getMinorTypeForArrowType(type), KdbTypes.valueOf(column.getMetadata().get(KdbMetadataHandler.KDBTYPE_KEY)));
        LOGGER.info("literal:" + String.valueOf(literal));
        return literal;
    }

    @VisibleForTesting
    static String toLiteral(Object value, ArrowType _type, Types.MinorType minorTypeForArrowType, KdbTypes kdbtype) {
LOGGER.info("type:" + String.valueOf(_type) + " minortype:" + String.valueOf(minorTypeForArrowType) + " kdbtype:" + String.valueOf(kdbtype) + " value:" + String.valueOf(value) + " valuetype:" + (value == null ? "null" : value.getClass().getName()));

            switch (minorTypeForArrowType) {
                case BIGINT:
                    if ( value == null )
                    {
                        return "0Nj";
                    }
                    else
                    {
                        return String.valueOf(value);
                    }
                case INT:
                    if ( value == null )
                    {
                        return "0Ni";
                    }
                    else
                    {
                        return ((Number) value).intValue() + "i";
                    }
                case SMALLINT:
                    if ( value == null )
                    {
                        return "0Nh";
                    }
                    else
                    {
                        return ((Number) value).shortValue() + "i";
                    }
                case TINYINT: //byte
                    if ( value == null )
                    {
                        return "0x00";
                    }
                    else
                    {
                        return ((Number) value).byteValue() + "i";
                    }
                case FLOAT8:
                    if ( kdbtype == KdbTypes.real_type)
                    {
                        if ( value == null )
                        {
                            return "0Ne";
                        }
                        else
                        {
                            return String.valueOf( ((Number) value).doubleValue() ) + "e"; 
                        }
                    }
                    else
                    {
                        if ( value == null )
                        {
                            return "0n";
                        }
                        else
                        {
                            return String.valueOf( ((Number) value).doubleValue() );
                        }
                    }
                case FLOAT4: //real
                    if ( value == null )
                    {
                        return "0Ne";
                    }
                    else
                    {
                        return String.valueOf( ((Number) value).floatValue() );
                    }
                case BIT: //boolean
                    if ( value == null )
                    {
                        return "0b";
                    }
                    else
                    {
                        return ((boolean) value) ? "1b" : "0b";
                    }
                case DATEDAY:
                    if ( value == null )
                    {
                        return "0Nd";
                    }
                    else
                    {
                        if (value instanceof Number)
                        {
                            int days_from_epoch = ((Number)value).intValue();
                            org.joda.time.LocalDateTime dateTime = EPOCH.minusDays(-days_from_epoch);
                            return DATE_FORMAT.get().print(dateTime);
                        }
                        else
                        {
                            org.joda.time.LocalDateTime dateTime = ((org.joda.time.LocalDateTime) value);
                            return DATE_FORMAT.get().print(dateTime);
                        }
                    }
                case DATEMILLI:
                    if ( value == null )
                    {
                        return "0Np";
                    }
                    else
                    {
                        org.joda.time.LocalDateTime timestamp = ((org.joda.time.LocalDateTime) value);
                        return DATE_FORMAT.get().print(timestamp) + "D" + TIME_FORMAT.get().print(timestamp);
                    }
                case VARCHAR:
                    if( kdbtype == KdbTypes.guid_type )
                    {
                        //guid
                        if ( value == null )
                        {
                            return "0Ng";
                        }
                        else
                        {
                            return "\"G\"$\"" + value + "\"";
                        }
                    }
                    else if( kdbtype == KdbTypes.char_type )
                    {
                        //char
                        if ( value == null )
                        {
                            return "\" \"";
                        }
                        else
                        {
                            return "\"" + value.toString() + "\"";
                        }
                    }
                    else if( kdbtype == KdbTypes.list_of_char_type )
                    {
                        throw new UnsupportedOperationException("list of char type cannot be pushed down to where statement");
                    }
                    else
                    {
                        //symbol
                        if ( value == null )
                        {
                            return "` ";
                        }
                        else
                        {
                            return "`" + String.valueOf(value);
                        }                        
                    }
                // case VARBINARY:
                //     return String.valueOf((byte[]) typeAndValue.getValue()); //or throw exception
                // case DECIMAL:
                //     ArrowType.Decimal decimalType = (ArrowType.Decimal) type;
                //     BigDecimal decimal = BigDecimal.valueOf((Long) value, decimalType.getScale());
                //     return decimal.toPlainString();
                default:
                    throw new UnsupportedOperationException(String.format("Can't handle type: %s, %s", _type, minorTypeForArrowType));
            }
    }

    @Override
    protected List<String> toConjuncts(List<Field> columns, Constraints constraints, List<TypeAndValue> accumulator, Map<String, String> partitionSplit)
    {
        List<String> conjuncts = new ArrayList<>();
        for (Field column : columns) {
            if (partitionSplit.containsKey(column.getName())) {
                continue; // Ignore constraints on partition name as RDBMS does not contain these as columns. Presto will filter these values.
            }
            if (KdbTypes.list_of_char_type.name().equals(column.getFieldType().getMetadata().get(KdbMetadataHandler.KDBTYPE_KEY)))
            {
                LOGGER.info("list of char column is excluded from where caluse");
                continue;
            }
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    conjuncts.add(toPredicate(column.getName(), column, valueSet, type, accumulator));
                }
            }
        }
        return conjuncts;
    }

    protected String toPredicate(String columnName, Field column, ValueSet valueSet, ArrowType type, List<TypeAndValue> accumulator)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        // TODO Add isNone and isAll checks once we have data on nullability.

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return toPredicateNull(columnName, column, type, accumulator);
            }

            // we don't need to add disjunction(OR (colname IS NULL)) because
            if (valueSet.isNullAllowed()) {
                disjuncts.add(toPredicateNull(columnName, column, type, accumulator));
            }

            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return toPredicateNull(columnName, column, type, accumulator);
            }

            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                rangeConjuncts.add(toPredicate(columnName, column, ">", range.getLow().getValue(), type, accumulator));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, column, ">=", range.getLow().getValue(), type, accumulator));
                                break;
                            case BELOW:
                                throw new IllegalArgumentException("Low marker should never use BELOW bound");
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                        }
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        switch (range.getHigh().getBound()) {
                            case ABOVE:
                                throw new IllegalArgumentException("High marker should never use ABOVE bound");
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, column, "<=", range.getHigh().getValue(), type, accumulator));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(columnName, column, "<", range.getHigh().getValue(), type, accumulator));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                    Preconditions.checkState(!rangeConjuncts.isEmpty());
                    disjuncts.add("(" + Joiner.on(" , ").join(rangeConjuncts) + ")");
                }
            }

            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                disjuncts.add(toPredicate(columnName, column, "=", Iterables.getOnlyElement(singleValues), type, accumulator));
            }
            else if (singleValues.size() > 1) {
                // for (Object value : singleValues) {
                //     accumulator.add(new TypeAndValue(type, value));
                // }
                List<Object> literals = Lists.newArrayListWithCapacity(singleValues.size());
                for(Object val : singleValues)
                    literals.add(toLiteral(val, type, columnName, column));
                String values = Joiner.on(",").join(literals);
                disjuncts.add(quote(columnName) + " IN (" + values + ")");
            }
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    protected String toPredicateNull(String columnName, Field column, ArrowType type, List<TypeAndValue> accumulator)
    {
        // accumulator.add(new TypeAndValue(type, value));
        return "(" + quote(columnName) + " = " + toLiteral(null, type, columnName, column) + ")";
    }


    protected String toPredicate(String columnName, Field column, String operator, Object value, ArrowType type, List<TypeAndValue> accumulator)
    {
        // accumulator.add(new TypeAndValue(type, value));
        return quote(columnName) + " " + operator + " " + toLiteral(value, type, columnName, column);
    }

    @Override
    protected String quote(String name)
    {
        return name;
    }
}

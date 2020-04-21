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

    KdbQueryStringBuilder(final String quoteCharacters)
    {
        super(quoteCharacters);
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
            return String.format(" FROM %s ", tableName);
        }

        return String.format(" FROM %s PARTITION(%s) ", tableName, partitionName);
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

    @VisibleForTesting
    static String toLiteral(Object value, ArrowType type) {        
        String literal = _toLiteral(value, type);
        LOGGER.info("literal:" + String.valueOf(literal));
        return literal;
    }

    static String _toLiteral(Object value, ArrowType type) {
            Types.MinorType minorTypeForArrowType = Types.getMinorTypeForArrowType(type);
LOGGER.info("type:" + type + " minortype:" + String.valueOf(minorTypeForArrowType) + " value:" + String.valueOf(value));

            switch (minorTypeForArrowType) {
                case BIGINT:
                    return String.valueOf(value);
                case INT:
                    return ((Number) value).intValue() + "i";
                case SMALLINT:
                    return ((Number) value).shortValue() + "i";
                case TINYINT:
                    return ((Number) value).byteValue() + "i";
                case FLOAT8:
                    return String.valueOf( ((Number) value).doubleValue() );
                case FLOAT4:
                    return String.valueOf( ((Number) value).floatValue() );
                case BIT:
                    return ((boolean) value) ? "1b" : "0b";
                case DATEDAY:
                    org.joda.time.LocalDateTime dateTime = ((org.joda.time.LocalDateTime) value);
                    return DATE_FORMAT.get().print(dateTime);
                case DATEMILLI:
                    org.joda.time.LocalDateTime timestamp = ((org.joda.time.LocalDateTime) value);
                    return DATE_FORMAT.get().print(timestamp) + "Z" + TIME_FORMAT.get().print(timestamp);
                case VARCHAR:
                    return String.valueOf(value);
                // case VARBINARY:
                //     return String.valueOf((byte[]) typeAndValue.getValue()); //or throw exception
                case DECIMAL:
                    ArrowType.Decimal decimalType = (ArrowType.Decimal) type;
                    BigDecimal decimal = BigDecimal.valueOf((Long) value, decimalType.getScale());
                    return decimal.toPlainString();
                default:
                    throw new UnsupportedOperationException(String.format("Can't handle type: %s, %s", type, minorTypeForArrowType));
            }
    }

    protected String toPredicate(String columnName, ValueSet valueSet, ArrowType type, List<TypeAndValue> accumulator)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        // TODO Add isNone and isAll checks once we have data on nullability.

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return String.format("(%s IS NULL)", columnName);
            }

            if (valueSet.isNullAllowed()) {
                disjuncts.add(String.format("(%s IS NULL)", columnName));
            }

            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return String.format("(%s IS NOT NULL)", columnName);
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
                                rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type, accumulator));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), type, accumulator));
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
                                rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type, accumulator));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), type, accumulator));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                    Preconditions.checkState(!rangeConjuncts.isEmpty());
                    disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
                }
            }

            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type, accumulator));
            }
            else if (singleValues.size() > 1) {
                // for (Object value : singleValues) {
                //     accumulator.add(new TypeAndValue(type, value));
                // }
                List<Object> literals = Lists.newArrayListWithCapacity(singleValues.size());
                for(Object val : singleValues)
                    literals.add(toLiteral(val, type));
                String values = Joiner.on(",").join(literals);
                disjuncts.add(quote(columnName) + " IN (" + values + ")");
            }
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    protected String toPredicate(String columnName, String operator, Object value, ArrowType type, List<TypeAndValue> accumulator)
    {
        // accumulator.add(new TypeAndValue(type, value));
        return quote(columnName) + " " + operator + " " + toLiteral(value, type);
    }

    @Override
    protected String quote(String name)
    {
        return name;
    }
}

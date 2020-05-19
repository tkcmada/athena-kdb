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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.connectors.athena.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.connectors.athena.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.manager.JDBCUtil;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.common.annotations.VisibleForTesting;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Data handler, user must have necessary permissions to read from necessary tables.
 */
public class KdbRecordHandler
        extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KdbRecordHandler.class);

    private static final String MYSQL_QUOTE_CHARACTER = "`";

    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    private static final int STRING_BUFFER_INITIAL_CAPACITY = 5000;

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link com.amazonaws.connectors.athena.jdbc.MultiplexingJdbcCompositeHandler} instead.
     */
    public KdbRecordHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(JdbcConnectionFactory.DatabaseEngine.KDB));
    }

    public KdbRecordHandler(DatabaseConnectionConfig databaseConnectionConfig)
    {
        this(databaseConnectionConfig, AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient(),
                new GenericJdbcConnectionFactory(databaseConnectionConfig, KdbMetadataHandler.JDBC_PROPERTIES), new KdbQueryStringBuilder(MYSQL_QUOTE_CHARACTER));
    }

    @VisibleForTesting
    KdbRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, final AmazonS3 amazonS3, final AWSSecretsManager secretsManager,
            final AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory, final JdbcSplitQueryBuilder jdbcSplitQueryBuilder)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
        LOGGER.info("jdbcSplitQueryBuilder:" + jdbcSplitQueryBuilder.getClass().getName());
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
            throws SQLException
    {
        LOGGER.info("constraints:" + (String.valueOf(constraints)));
        LOGGER.info("split:" + (String.valueOf(split)));
        PreparedStatement preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableName.getSchemaName(), tableName.getTableName(), schema, constraints, split);
LOGGER.info("pstmt:" + String.valueOf(preparedStatement));
        // Disable fetching all rows.
        preparedStatement.setFetchSize(Integer.MIN_VALUE);

        return preparedStatement;
    }

    @Override
    public void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest, QueryStatusChecker queryStatusChecker)
    {
        LOGGER.info("{}: Catalog: {}, table {}, splits {}", readRecordsRequest.getQueryId(), readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                readRecordsRequest.getSplit().getProperties());
        try (Connection connection = jdbcConnectionFactory.getConnection(getCredentialProvider())) {
            connection.setAutoCommit(false); // For consistency. This is needed to be false to enable streaming for some database types.
            try (PreparedStatement preparedStatement = buildSplitSql(connection, readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                    readRecordsRequest.getSchema(), readRecordsRequest.getConstraints(), readRecordsRequest.getSplit());
                    ResultSet resultSet = preparedStatement.executeQuery()) {
                Map<String, String> partitionValues = readRecordsRequest.getSplit().getProperties();

                GeneratedRowWriter.RowWriterBuilder rowWriterBuilder = GeneratedRowWriter.newBuilder(readRecordsRequest.getConstraints());
                for (Field next : readRecordsRequest.getSchema().getFields()) {
                    Extractor extractor = makeExtractor(next, resultSet, partitionValues);
                    rowWriterBuilder.withExtractor(next.getName(), extractor);
                }

                GeneratedRowWriter rowWriter = rowWriterBuilder.build();
                int rowsReturnedFromDatabase = 0;
                while (resultSet.next()) {
                    if (!queryStatusChecker.isQueryRunning()) {
                        return;
                    }
                    blockSpiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, resultSet) ? 1 : 0);
                    rowsReturnedFromDatabase++;
                }
                LOGGER.info("{} rows returned by database.", rowsReturnedFromDatabase);

                connection.commit();
            }
        }
        catch (SQLException sqlException) {
            throw new RuntimeException(sqlException.getErrorCode() + ": " + sqlException.getMessage(), sqlException);
        }
    }

    @Override
    protected Float8Extractor newFloat8Extractor(final ResultSet resultSet, final String fieldName, final Field field)
    {
        return (Float8Extractor) (Object context, org.apache.arrow.vector.holders.NullableFloat8Holder dst) ->
        {
            final String kdbtype = field.getFieldType().getMetadata().get(KdbMetadataHandler.KDBTYPE_KEY);
            if (KdbTypes.real_type.name().equals(kdbtype)) {
                final float f = resultSet.getFloat(fieldName);
                dst.value = Double.parseDouble("" + f); //do not just cast from float to double as it would contain fraction
                dst.isSet = resultSet.wasNull() ? 0 : 1;
                LOGGER.info("Float8Extractor(float) " + String.valueOf(fieldName) + " " + dst.value + " float value=" + f);
            }
            else {
                dst.value = resultSet.getDouble(fieldName);
                dst.isSet = resultSet.wasNull() ? 0 : 1;
                LOGGER.info("Float8Extractor(double) " + String.valueOf(fieldName) + " " + dst.value + " double value=" + resultSet.getDouble(fieldName));
            }
        };
    }

    @Override
    protected VarCharExtractor newVarcharExtractor(final ResultSet resultSet, final String fieldName, final Field field)
    {
        return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
        {
            Object value = resultSet.getObject(fieldName);
            if (value != null) {
                final char kdbtypechar = KdbMetadataHandler.getKdbTypeChar(field);
                switch(kdbtypechar) {
                    case 'n': //timespan
                        final String timespanstr = value.toString() + "000000000";
                        //00:00:00.123456789
                        dst.value = timespanstr.substring(0, 18);
                        break;
                    case 'p': //timestamp
                        final Timestamp timestamp = (Timestamp)value;
                        dst.value = KdbQueryStringBuilder.toLiteral(timestamp, Types.MinorType.VARCHAR, KdbTypes.timestamp_type);
                        break;
                    case 'S': //list of symbol
                        final String[] symbols = (String[]) value;
                        dst.value = toVarChar(symbols);
                        break;
                    case 'P': //list of timestamp
                        final Timestamp[] timestamps = (Timestamp[]) value;
                        dst.value = toVarChar(timestamps);
                        break;
                    case 'X': //list of byte
                        final byte[] bytes = (byte[]) value;
                        dst.value = toVarChar(bytes);
                        break;
                    case 'I': //list of int
                        final int[] ints = (int[]) value;
                        dst.value = toVarChar(ints);
                        break;
                    case 'J': //list of long
                        final long[] longs = (long[]) value;
                        dst.value = toVarChar(longs);
                        break;
                    case 'F': //list of float
                        final double[] doubles = (double[]) value;
                        dst.value = toVarChar(doubles);
                        break;
                    default:
                        dst.value = value.toString();
                }
            }
            dst.isSet = resultSet.wasNull() ? 0 : 1;
        };
    }

    @VisibleForTesting
    static String toVarChar(int[] a)
    {
        //StringBuilder with pre allocated is faster than Stream.
        StringBuilder sb = new StringBuilder(STRING_BUFFER_INITIAL_CAPACITY);
        for (int i = 0; i < a.length; i++)
        {
            if (i > 0)
                sb.append(" ");
            sb.append(a[i]);
        }
        return sb.toString();
    }

    @VisibleForTesting
    static String toVarChar(long[] a)
    {
        //StringBuilder with pre allocated is faster than Stream.
        StringBuilder sb = new StringBuilder(STRING_BUFFER_INITIAL_CAPACITY);
        for (int i = 0; i < a.length; i++)
        {
            if (i > 0)
                sb.append(" ");
            sb.append(a[i]);
        }
        return sb.toString();
    }

    @VisibleForTesting
    static String toVarChar(double[] a)
    {
        //StringBuilder with pre allocated is faster than Stream.
        StringBuilder sb = new StringBuilder(STRING_BUFFER_INITIAL_CAPACITY);
        for (int i = 0; i < a.length; i++)
        {
            if (i > 0)
                sb.append(" ");
            sb.append(a[i]);
        }
        return sb.toString();
    }

    @VisibleForTesting
    static String toVarChar(byte[] a)
    {
        final char[] chars = Hex.encodeHex(a);
        StringBuilder s = new StringBuilder(chars.length + 2);
        s.append("0x");
        s.append(chars);
        return s.toString();
    }

    @VisibleForTesting
    static String toVarChar(Timestamp[] timestamps)
    {
        //StringBuilder with pre allocated is faster than Stream.
        StringBuilder sb = new StringBuilder(STRING_BUFFER_INITIAL_CAPACITY);
        for (int i = 0; i < timestamps.length; i++)
        {
            if (i > 0)
                sb.append(" ");
            sb.append(KdbQueryStringBuilder.toLiteral(timestamps[i], Types.MinorType.VARCHAR, KdbTypes.timestamp_type));
        }
        return sb.toString();
    }

    @VisibleForTesting
    static String toVarChar(String[] symbols)
    {
        //StringBuilder with pre allocated is faster than Stream.
        StringBuilder sb = new StringBuilder(STRING_BUFFER_INITIAL_CAPACITY);
        for (int i = 0; i < symbols.length; i++)
        {
            sb.append(KdbQueryStringBuilder.toLiteral(symbols[i], Types.MinorType.VARCHAR, KdbTypes.symbol_type));
        }
        return sb.toString();
    }
}

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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.connectors.athena.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.connectors.athena.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.manager.JDBCUtil;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.connectors.athena.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handles metadata for MySQL. User must have access to `schemata`, `tables`, `columns`, `partitions` tables in
 * information_schema.
 */
public class KdbMetadataHandler
        extends JdbcMetadataHandler
{
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String GET_PARTITIONS_QUERY = "SELECT DISTINCT partition_name FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ? " +
            "AND partition_name IS NOT NULL";
    static final String BLOCK_PARTITION_COLUMN_NAME = "partition_name";
    static final String ALL_PARTITIONS = "*";
    static final String PARTITION_COLUMN_NAME = "partition_name";
    private static final Logger LOGGER = LoggerFactory.getLogger(KdbMetadataHandler.class);
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link com.amazonaws.connectors.athena.jdbc.MultiplexingJdbcCompositeHandler} instead.
     */
    public KdbMetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(JdbcConnectionFactory.DatabaseEngine.KDB));
    }

    /**
     * Used by Mux.
     */
    public KdbMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        super(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES));
    }

    @VisibleForTesting
    protected KdbMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
            AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory);
    }

    @Override
    protected Set<String> listDatabaseNames(final Connection jdbcConnection)
            throws SQLException
    {
//        try (ResultSet resultSet = jdbcConnection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            schemaNames.add("schema1");
        //     while (resultSet.next()) {
        //         String schemaName = resultSet.getString("TABLE_SCHEM");
        //         // skip internal schemas
        //         if (!schemaName.equals("information_schema")) {
        //             schemaNames.add(schemaName);
        //         }
        //     }
            return schemaNames.build();
        // }
    }

    @Override
    protected List<TableName> listTables(final Connection jdbcConnection, final String databaseName)
            throws SQLException
    {
        try ( Statement stmt = jdbcConnection.createStatement() ) {
            try (ResultSet resultSet = stmt.executeQuery("q) flip ( `TABLE_NAME`dummy ! ( tables[]; tables[] ) )") ) {
                ImmutableList.Builder<TableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    list.add(getSchemaTableName(resultSet));
                }
                return list.build();
            }
        }
    }

    @Override
    protected TableName getSchemaTableName(final ResultSet resultSet)
            throws SQLException
    {
        return new TableName(
                "schema1",
                resultSet.getString("TABLE_NAME"));
    }

    @Override
    protected Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
            throws SQLException
    {
        LOGGER.info("getSchema...");
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        try ( Statement stmt = jdbcConnection.createStatement() ) {
            try ( ResultSet rs = stmt.executeQuery("q) flip `COLUMN_NAME`COLUMN_TYPE!(cols t; (value meta t)[;`t] )") ) {
                while (rs.next()) {
                    String colname = rs.getString("COLUMN_NAME");
                    Character coltypeobj = (Character)rs.getObject("COLUMN_TYPE");
                    char coltype = (char)coltypeobj;
                    LOGGER.info("schema column mapping..." + colname + " " + coltype);
                    switch (coltype) {
                        case 'b':
                            schemaBuilder.addBitField(colname);
                            break;
                        case 'x':
                            schemaBuilder.addTinyIntField(colname);
                            break;
                        case 'h':
                            schemaBuilder.addSmallIntField(colname);
                            break;
                        case 'i':
                            schemaBuilder.addIntField(colname);
                            break;
                        case 'j':
                            schemaBuilder.addBigIntField(colname);
                            break;
                        case 'e':
                            schemaBuilder.addFloat4Field(colname);
                            break;
                        case 'f':
                            schemaBuilder.addFloat8Field(colname);
                            break;
                        case 'c':
                            schemaBuilder.addTinyIntField(colname); //TODO : character is preffered
                            break;
                        case 's':
                            schemaBuilder.addStringField(colname);
                            break;
                        // case 'p': //timestamp
                        //     continue;
                        //     // schemaBuilder.addDateMilliField(colname);
                        //     // schemaBuilder.addField("z", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"));
                        //     // break;
                        // case 't': //time
                        //     continue;
                        //     // schemaBuilder.addDateMilliField(colname);
                        //     // schemaBuilder.addField("t", new ArrowType.Time(TimeUnit.NANOSECOND, 128)); //only 8, 16, 32, 64, or 128 bits supported
                        //     // break;
                        case 'd':
                            schemaBuilder.addDateDayField(colname);
                            break;
                        default:
                            LOGGER.error("getSchema: Unable to map type for column[" + colname + "] to a supported type, attempted " + coltype);
                    }
                }
                
                
            }
        }

// q)(2i;2.3;`qwe;2000.01.02;12:34:56.000;2000.01.02D12:34:56.000000000)
// (2i;2.3;`qwe;2000.01.02;12:34:56.000;2000.01.02D12:34:56.000000000)

// q)t
// x f   s   d          t            z                            
// ---------------------------------------------------------------
// 2 2.3 qwe 2000.01.02 12:34:56.000 2000.01.02D12:34:56.000000000
// q)metat
// 'metat
//   [0]  metat
//        ^
// q)meta t
// c| t f a
// -| -----
// x| i    
// f| f    
// s| s    
// d| d    
// t| t    
// z| p    

        // try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData())) {
        //     boolean found = false;
        //     while (resultSet.next()) {
        //         ArrowType columnType = JdbcArrowTypeConverter.toArrowType(
        //                 resultSet.getInt("DATA_TYPE"),
        //                 resultSet.getInt("COLUMN_SIZE"),
        //                 resultSet.getInt("DECIMAL_DIGITS"));
        //         String columnName = resultSet.getString("COLUMN_NAME");
        //         if (columnType != null && SupportedTypes.isSupported(columnType)) {
        //             schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
        //             found = true;
        //         }
        //         else {
        //             LOGGER.error("getSchema: Unable to map type for column[" + columnName + "] to a supported type, attempted " + columnType);
        //         }
        //     }

        //     if (!found) {
        //         throw new RuntimeException("Could not find table in " + tableName.getSchemaName());
        //     }

            // add partition columns
            partitionSchema.getFields().forEach(schemaBuilder::addField);

            Schema s = schemaBuilder.build();
            for ( Field f : s.getFields() ) {
                Types.MinorType mtype = Types.getMinorTypeForArrowType(f.getType());
                LOGGER.info(String.format("%s %s %s", f.getName(), f.getType(), mtype));
            }
            return s;
        // }
    }


    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    @Override
    public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest getTableLayoutRequest, QueryStatusChecker queryStatusChecker)
    {
        LOGGER.info("{}: Schema {}, table {}", getTableLayoutRequest.getQueryId(), getTableLayoutRequest.getTableName().getSchemaName(),
                getTableLayoutRequest.getTableName().getTableName());
        // try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
        //     final String escape = connection.getMetaData().getSearchStringEscape();

        //     List<String> parameters = Arrays.asList(getTableLayoutRequest.getTableName().getTableName(), getTableLayoutRequest.getTableName().getSchemaName());
            // try (PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(GET_PARTITIONS_QUERY).withParameters(parameters).build();
            //         ResultSet resultSet = preparedStatement.executeQuery()) {
            //     // Return a single partition if no partitions defined
            //     if (!resultSet.next()) {
                    blockWriter.writeRows((Block block, int rowNum) -> {
                        block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                        LOGGER.info("Adding partition {}", ALL_PARTITIONS);
                        //we wrote 1 row so we return 1
                        return 1;
                    });
            //     }
            //     else {
            //         do {
            //             final String partitionName = resultSet.getString(PARTITION_COLUMN_NAME);

            //             // 1. Returns all partitions of table, we are not supporting constraints push down to filter partitions.
            //             // 2. This API is not paginated, we could use order by and limit clause with offsets here.
            //             blockWriter.writeRows((Block block, int rowNum) -> {
            //                 block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionName);
            //                 LOGGER.info("Adding partition {}", partitionName);
            //                 //we wrote 1 row so we return 1
            //                 return 1;
            //             });
            //         }
            //         while (resultSet.next() && queryStatusChecker.isQueryRunning());
            //     }
            // }
        // }
        // catch (SQLException sqlException) {
        //     throw new RuntimeException(sqlException.getErrorCode() + ": " + sqlException.getMessage(), sqlException);
        // }
    }

    @Override
    public GetSplitsResponse doGetSplits(
            final BlockAllocator blockAllocator, final GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("{}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
        int partitionContd = decodeContinuationToken(getSplitsRequest);
        Set<Split> splits = new HashSet<>();
        Block partitions = getSplitsRequest.getPartitions();

        // TODO consider splitting further depending on #rows or data size. Could use Hash key for splitting if no partitions.
        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            FieldReader locationReader = partitions.getFieldReader(BLOCK_PARTITION_COLUMN_NAME);
            locationReader.setPosition(curPartition);

            SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);

            LOGGER.info("{}: Input partition is {}", getSplitsRequest.getQueryId(), locationReader.readText());

            Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                    .add(BLOCK_PARTITION_COLUMN_NAME, String.valueOf(locationReader.readText()));

            splits.add(splitBuilder.build());

            if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
                return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, encodeContinuationToken(curPartition));
            }
        }

        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.valueOf(request.getContinuationToken());
        }

        //No continuation token present
        return 0;
    }

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }
}

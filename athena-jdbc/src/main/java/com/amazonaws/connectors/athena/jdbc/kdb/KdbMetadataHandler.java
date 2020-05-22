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
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
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
    public static final String KDBTYPE_KEY = "kdbtype";
    public static final String KDBTYPECHAR_KEY = "kdbtypechar";
    public static final String DEFAULT_SCHEMA_NAME = "schema1";

    private static boolean isListMappedToArray = true;

    public static boolean isListMappedToArray() { return isListMappedToArray; }
    public static void setListMappedToArray(boolean value) { isListMappedToArray = value; }
    
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
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        schemaNames.add(DEFAULT_SCHEMA_NAME); //kdb+ doesn't have schemas so returns default database.
        return schemaNames.build();
    }

    @Override
    protected List<TableName> listTables(final Connection jdbcConnection, final String databaseName)
            throws SQLException
    {
        try (Statement stmt = jdbcConnection.createStatement()) {
            final String SCHEMA_QUERY = "q) flip ( `TABLE_NAME`TABLE_SCHEM ! ( tables[]; (count(tables[]))#(enlist \"" + DEFAULT_SCHEMA_NAME + "\") ) )";
            try (ResultSet resultSet = stmt.executeQuery(SCHEMA_QUERY)) {
                ImmutableList.Builder<TableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    LOGGER.info(String.format("list table:%s %s", resultSet.getObject("TABLE_SCHEM"), resultSet.getObject("TABLE_NAME")));
                    list.add(getSchemaTableName(resultSet));
                }
                return list.build();
            }
        }
    }

    @Override
    protected Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
            throws SQLException
    {
        //Plese note that only following are supported in Athena as of 2020.05
        // BIT(Types.MinorType.BIT),
        // DATEMILLI(Types.MinorType.DATEMILLI),
        // DATEDAY(Types.MinorType.DATEDAY),
        // FLOAT8(Types.MinorType.FLOAT8),
        // FLOAT4(Types.MinorType.FLOAT4),
        // INT(Types.MinorType.INT),
        // TINYINT(Types.MinorType.TINYINT),
        // SMALLINT(Types.MinorType.SMALLINT),
        // BIGINT(Types.MinorType.BIGINT),
        // VARBINARY(Types.MinorType.VARBINARY),
        // DECIMAL(Types.MinorType.DECIMAL),
        // VARCHAR(Types.MinorType.VARCHAR),
        // STRUCT(Types.MinorType.STRUCT),
        // LIST(Types.MinorType.LIST);

        LOGGER.info("getSchema...");
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        try (Statement stmt = jdbcConnection.createStatement()) {
            String tbl = tableName.getTableName();
            try (ResultSet rs = stmt.executeQuery("q) flip `COLUMN_NAME`COLUMN_TYPE!(cols " + tbl + "; (value meta " + tbl + ")[;`t] )")) {
                while (rs.next()) {
                    String colname = rs.getString("COLUMN_NAME");
                    Character coltypeobj = (Character) rs.getObject("COLUMN_TYPE");
                    char coltype = (char) coltypeobj;
                    LOGGER.info("schema column mapping..." + colname + " " + coltype);
                    switch (coltype) {
                        case 'b':
                            schemaBuilder.addField(newField(colname, Types.MinorType.BIT, KdbTypes.bit_type));
                            break;
                        case 'x':
                            schemaBuilder.addField(newField(colname, Types.MinorType.TINYINT, KdbTypes.byte_type));
                            break;
                        case 'h':
                            schemaBuilder.addField(newField(colname, Types.MinorType.SMALLINT, KdbTypes.short_type));
                            break;
                        case 'i':
                            schemaBuilder.addField(newField(colname, Types.MinorType.INT, KdbTypes.int_type));
                            break;
                        case 'j':
                            schemaBuilder.addField(newField(colname, Types.MinorType.BIGINT, KdbTypes.long_type));
                            break;
                        case 'e': //real is mapped to Float8 but actual kdb type is real
                            schemaBuilder.addField(newField(colname, Types.MinorType.FLOAT8, KdbTypes.real_type));
                            break;
                        case 'f':
                            schemaBuilder.addField(newField(colname, Types.MinorType.FLOAT8, KdbTypes.float_type));
                            break;
                        case 'c': //char is mapped to VARCHAR because Athena doesn't have 
                            schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.char_type));
                            break;
                        case 's': //symbol
                            schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.symbol_type));
                            break;
                        case 'C': //list of char
                            schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_char_type));
                            break;
                        case 'g': //guid
                            schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.guid_type));
                            break;
                        case 'p': //timestamp
                            //Athena doesn't support DATENANO so map to VARCHAR for now
                            schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.timestamp_type));
                            break;
                        // case 't': //time //Athena doesn't support TIMEMILL
                        //     //Jdbc automatically map this column to java.sql.Time which has only sec precision
                        //     schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.time_type));
                        //     break;
                        case 'n': //timespan //Athena doesn't support TIMENANO
                            //just map to VARCHAR for now
                            schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.timespan_type));
                            break;
                        case 'd':
                            schemaBuilder.addField(newField(colname, Types.MinorType.DATEDAY, KdbTypes.date_type));
                            break;
                        case 'J':
                            schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_long_type));
                            break;
                        case 'I':
                            schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_int_type));
                            break;
                        case 'X':
                            schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_byte_type));
                            break;
                        case 'F':
                            if (isListMappedToArray())
                            {
                                schemaBuilder.addField(newListField(colname, KdbTypes.list_of_float_type, Types.MinorType.FLOAT8, KdbTypes.float_type));
                            }
                            else
                            {
                                schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_float_type));
                            }
                            break;
                        case 'S':
                            schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_symbol_type));
                            break;
                        case 'P':
                            schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_timestamp_type));
                            break;
                        default:
                            LOGGER.error("getSchema: Unable to map type for column[" + colname + "] to a supported type, attempted '" + coltype + "'");
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
            for (Field f : s.getFields()) {
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
                .addField(newField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR, KdbTypes.list_of_char_type));
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

    @VisibleForTesting
    static Field newField(String colname, Types.MinorType minorType, KdbTypes kdbtype)
    {
        final Map<String, String> metadata = ImmutableMap.<String, String>builder()
            .put(KDBTYPE_KEY    , kdbtype == null ? "null" : kdbtype.name())
            .put(KDBTYPECHAR_KEY, kdbtype == null ? " " : String.valueOf(kdbtype.kdbtype))
            .build();
        FieldType fieldType = new FieldType(true, minorType.getType(), null, metadata);
        return new Field(colname, fieldType, null);
    }

    @VisibleForTesting
    static Field newListField(String colname, KdbTypes listkdbtype, Types.MinorType primitive_minorType, KdbTypes primitive_kdbtype)
    {
        final Map<String, String> metadata = ImmutableMap.<String, String>builder()
            .put(KDBTYPE_KEY    , listkdbtype == null ? "null" : listkdbtype.name())
            .put(KDBTYPECHAR_KEY, listkdbtype == null ? " " : String.valueOf(listkdbtype.kdbtype))
            .build();
            
        FieldType fieldtype = new FieldType(false, new ArrowType.List(), null, metadata);

        Field baseField = newField("", primitive_minorType, primitive_kdbtype);
        Field listfield = new Field(colname,
                fieldtype,
                Collections.singletonList(baseField));
        return listfield;
    }

    @VisibleForTesting
    static char getKdbTypeChar(Field field)
    {
        return field.getMetadata().get(KDBTYPECHAR_KEY).charAt(0);
    }
}

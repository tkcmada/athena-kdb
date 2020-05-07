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

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.connectors.athena.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.connectors.athena.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

public class KdbRecordHandlerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KdbRecordHandlerTest.class);

    private KdbRecordHandler mySqlRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private KdbMetadataHandler metadataHandler;
    private KdbMetadataHelper metadataHelper;
    private AmazonS3 amazonS3;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;
    private TableName tableName;
    private Schema schema;

    @Before
    public void setup()
    {
        this.amazonS3 = Mockito.mock(AmazonS3.class);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(Mockito.mock(JdbcCredentialProvider.class))).thenReturn(this.connection);
        this.metadataHandler = Mockito.mock(KdbMetadataHandler.class);
        this.metadataHelper = Mockito.mock(KdbMetadataHelper.class);
        Mockito.when(metadataHelper.getKdbType("g")).thenReturn(KdbTypes.guid_type);
        Mockito.when(metadataHelper.getKdbType("r")).thenReturn(KdbTypes.real_type);
        Mockito.when(metadataHelper.getKdbType("str")).thenReturn(KdbTypes.list_of_char_type);
        Mockito.when(metadataHelper.getKdbType("c")).thenReturn(KdbTypes.char_type);
        jdbcSplitQueryBuilder = new KdbQueryStringBuilder(metadataHelper, "`");
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", JdbcConnectionFactory.DatabaseEngine.MYSQL,
                "mysql://jdbc:mysql://hostname/user=A&password=B");

        this.mySqlRecordHandler = new KdbRecordHandler(metadataHelper, databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder);

        tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("r"       , Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol5", Types.MinorType.SMALLINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol6", Types.MinorType.TINYINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol7", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol8", Types.MinorType.BIT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol9", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol10", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("g"        , Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("str"      , Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("c"        , Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition_name", Types.MinorType.VARCHAR.getType()).build());
        schema = schemaBuilder.build();
    }

    @Test
    public void makeExtractor() throws Exception {
        LOGGER.info("makeExtractor starting");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.VARCHAR.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.BIGINT.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol4", Types.MinorType.FLOAT4.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol5", Types.MinorType.SMALLINT.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol6", Types.MinorType.TINYINT.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol7", Types.MinorType.FLOAT8.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol8", Types.MinorType.BIT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("d", Types.MinorType.DATEDAY.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol10", Types.MinorType.DATEMILLI.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("g"        , Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition_name", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();  
        schema.getFields();
        ResultSet rs = Mockito.mock(ResultSet.class); 
        Mockito.when(rs.getDate("f")).thenReturn(new Date(1970, 0, 1));
        DateDayExtractor extractor = (DateDayExtractor)this.mySqlRecordHandler.makeExtractor(schema.getFields().get(0), rs, Maps.newHashMap()); 
        NullableDateDayHolder holder = new NullableDateDayHolder();
        extractor.extract(null, holder);
        Assert.assertEquals(1, holder.isSet);
        Assert.assertEquals(0, holder.value);
    }

    @Test
    public void newFloat8Extractor() throws Exception
    {
        LOGGER.info("newFloat8Extractor starting...");
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getFloat("r")).thenReturn(1.4f);
        NullableFloat8Holder dst = new NullableFloat8Holder();
        this.mySqlRecordHandler.newFloat8Extractor(rs, "r").extract(null, dst);
        Assert.assertEquals(1.4, dst.value, 0.000000001);
    }

    @Test
    public void newVarCharExtractor_char() throws Exception
    {
        LOGGER.info("newVarCharExtractor_char starting...");
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("c")).thenReturn('w');
        NullableVarCharHolder dst = new NullableVarCharHolder();
        this.mySqlRecordHandler.newVarcharExtractor(rs, "c").extract(null, dst);
        Assert.assertEquals("w", dst.value);
    }

    @Test
    public void newVarCharExtractor_str() throws Exception
    {
        LOGGER.info("newVarCharExtractor_str starting...");
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("str")).thenReturn("abc");
        NullableVarCharHolder dst = new NullableVarCharHolder();
        this.mySqlRecordHandler.newVarcharExtractor(rs, "str").extract(null, dst);
        Assert.assertEquals("abc", dst.value);
    }

    @Test
    public void buildSplitSql()
            throws SQLException
    {
        LOGGER.info("buildSplitSql starting");

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition_name", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition_name"))).thenReturn("p0");

        Range range1a = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1a.isSingleValue()).thenReturn(true);
        Mockito.when(range1a.getLow().getValue()).thenReturn(1);
        Range range1b = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1b.isSingleValue()).thenReturn(true);
        Mockito.when(range1b.getLow().getValue()).thenReturn(2);
        ValueSet valueSet1 = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet1.getRanges().getOrderedRanges()).thenReturn(ImmutableList.of(range1a, range1b));

        ValueSet valueSet2 = getSingleValueSet("abc");
        ValueSet valueSet3 = getRangeSet(Marker.Bound.ABOVE, 2L, Marker.Bound.EXACTLY, 20L);
        ValueSet valueSet4 = getSingleValueSet(1.5); //real
        ValueSet valueSet5 = getSingleValueSet(1);
        ValueSet valueSet6 = getSingleValueSet(0);
        ValueSet valueSet7 = getSingleValueSet(1.2d);
        ValueSet valueSet8 = getSingleValueSet(true);
        ValueSet valueSet9 = getSingleValueSet(new LocalDateTime(2020, 1, 1, 0, 0, 0, 0));
        ValueSet valueSet10 = getSingleValueSet(new LocalDateTime(2020, 1, 1, 2, 3, 4, 5));
        ValueSet valueSet11 = getSingleValueSet("1234-5678");
        ValueSet valueSet_str = getSingleValueSet("xyz");
        ValueSet valueSet_c = getSingleValueSet("w");

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol1", valueSet1)
                .put("testCol2", valueSet2)
                .put("testCol3", valueSet3)
                .put("r"       , valueSet4)
                .put("testCol5", valueSet5)
                .put("testCol6", valueSet6)
                .put("testCol7", valueSet7)
                .put("testCol8", valueSet8)
                .put("testCol9", valueSet9)
                .put("testCol10", valueSet10)
                .put("g"        , valueSet11)
                .put("str"      , valueSet_str)
                .put("c"        , valueSet_c)
                .build());

        String expectedSql = "q) select testCol1, testCol2, testCol3, r, testCol5, testCol6, testCol7, testCol8, testCol9, testCol10, g, str, c from testTable PARTITION(p0)  where (testCol1 IN (1i,2i)) , (testCol2 = `abc) , ((testCol3 > 2 , testCol3 <= 20)) , (r = 1.5e) , (testCol5 = 1i) , (testCol6 = 0i) , (testCol7 = 1.2) , (testCol8 = 1b) , (testCol9 = 2020.01.01) , (testCol10 = 2020.01.01D02:03:04.005000000) , (g = \"G\"$\"1234-5678\") , (c = \"w\")";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.mySqlRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void buildSplitSql_null()
            throws SQLException
    {
        LOGGER.info("buildSplitSql_null starting");

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition_name", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition_name"))).thenReturn("p0");

        ValueSet valueSet1 = getSingleValueSetOnlyNull();
        ValueSet valueSet2 = getSingleValueSetOnlyNull();
        ValueSet valueSet3 = getSingleValueSetOnlyNull();
        ValueSet valueSet4 = getSingleValueSetOnlyNull();
        ValueSet valueSet5 = getSingleValueSetOnlyNull();
        ValueSet valueSet6 = getSingleValueSetOnlyNull();
        ValueSet valueSet7 = getSingleValueSetOnlyNull();
        ValueSet valueSet8 = getSingleValueSetOnlyNull();
        ValueSet valueSet9 = getSingleValueSetOnlyNull();
        ValueSet valueSet10 = getSingleValueSetOnlyNull();
        ValueSet valueSet11 = getSingleValueSetOnlyNull();
        ValueSet valueSet_str = getSingleValueSetOnlyNull();
        ValueSet valueSet_c = getSingleValueSetOnlyNull();

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol1", valueSet1)
                .put("testCol2", valueSet2)
                .put("testCol3", valueSet3)
                .put("r"       , valueSet4)
                .put("testCol5", valueSet5)
                .put("testCol6", valueSet6)
                .put("testCol7", valueSet7)
                .put("testCol8", valueSet8)
                .put("testCol9", valueSet9)
                .put("testCol10", valueSet10)
                .put("g"        , valueSet11)
                .put("str"      , valueSet_str)
                .put("c"        , valueSet_c)
                .build());

        String expectedSql = "q) select testCol1, testCol2, testCol3, r, testCol5, testCol6, testCol7, testCol8, testCol9, testCol10, g, str, c from testTable PARTITION(p0)  where (testCol1 = 0Ni) , (testCol2 = ` ) , (testCol3 = 0Nj) , (r = 0Ne) , (testCol5 = 0Nh) , (testCol6 = 0x00) , (testCol7 = 0n) , (testCol8 = 0b) , (testCol9 = 0Nd) , (testCol10 = 0Np) , (g = 0Ng) , (c = \" \")";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.mySqlRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        // Mockito.verify(preparedStatement, Mockito.times(1)).setInt(10, 1);
        // Mockito.verify(preparedStatement, Mockito.times(1)).setInt(11, 2);
        // Mockito.verify(preparedStatement, Mockito.times(1)).setString(6, "1");
        // Mockito.verify(preparedStatement, Mockito.times(1)).setString(7, "10");
        // Mockito.verify(preparedStatement, Mockito.times(1)).setLong(4, 2L);
        // Mockito.verify(preparedStatement, Mockito.times(1)).setLong(5, 20L);
        // Mockito.verify(preparedStatement, Mockito.times(1)).setFloat(9, 1.1F);
        // Mockito.verify(preparedStatement, Mockito.times(1)).setShort(8, (short) 1);
        // Mockito.verify(preparedStatement, Mockito.times(1)).setByte(2, (byte) 0);
        // Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(1, 1.2d);
        // Mockito.verify(preparedStatement, Mockito.times(1)).setBoolean(3, true);
    }


    static private ValueSet getSingleValueSet(Object value) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    static private ValueSet getSingleValueSetOnlyNull() {
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.isNullAllowed()).thenReturn(true);
        Mockito.when(valueSet.isNone()).thenReturn(true); //collection is empty
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.emptyList());
        return valueSet;
    }

    static private ValueSet getRangeSet(Marker.Bound lowerBound, Object lowerValue, Marker.Bound upperBound, Object upperValue) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(false);
        Mockito.when(range.getLow().getBound()).thenReturn(lowerBound);
        Mockito.when(range.getLow().getValue()).thenReturn(lowerValue);
        Mockito.when(range.getHigh().getBound()).thenReturn(upperBound);
        Mockito.when(range.getHigh().getValue()).thenReturn(upperValue);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }
}

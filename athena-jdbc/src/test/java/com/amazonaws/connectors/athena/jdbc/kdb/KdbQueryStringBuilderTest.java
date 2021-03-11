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

import java.sql.Timestamp;

import org.apache.arrow.vector.types.Types.MinorType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KdbQueryStringBuilderTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KdbQueryStringBuilderTest.class);

    // private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    // private KdbMetadataHandler metadataHandler;

    @Before
    public void setup()
    {
        // this.metadataHandler = Mockito.mock(KdbMetadataHandler.class);
        // Mockito.when(metadataHandler.isGUID("g")).thenReturn(true);
        // this.jdbcSplitQueryBuilder = new KdbQueryStringBuilder(metadataHandler, "`");
    }

    @Test
    public void toLiteral() throws Exception {
        LOGGER.info("toLiteral starting");

        Assert.assertEquals("1.5e"               , KdbQueryStringBuilder.toLiteral(1.5, MinorType.FLOAT8, KdbTypes.real_type));
        Assert.assertEquals("1970.01.02"         , KdbQueryStringBuilder.toLiteral(1, MinorType.DATEDAY, null));
        Assert.assertEquals("1970.01.04D00:00:00.004000000", KdbQueryStringBuilder.toLiteral(new org.joda.time.LocalDateTime(1970, 1, 4, 0, 0, 0, 4), MinorType.DATEMILLI, null));
        Assert.assertEquals("\"G\"$\"1234-5678\"", KdbQueryStringBuilder.toLiteral("1234-5678", MinorType.VARCHAR, KdbTypes.guid_type));

        //time
        Assert.assertEquals("00:00:00.001",
            KdbQueryStringBuilder.toLiteral("00:00:00.001", MinorType.VARCHAR, KdbTypes.time_type));
        Assert.assertEquals("0Nt",
            KdbQueryStringBuilder.toLiteral(null          , MinorType.VARCHAR, KdbTypes.time_type));

        //timespan
        Assert.assertEquals("00:00:00.001002003",
            KdbQueryStringBuilder.toLiteral("00:00:00.001002003", MinorType.VARCHAR, KdbTypes.timespan_type));
        Assert.assertEquals("0Nn",
            KdbQueryStringBuilder.toLiteral(null                , MinorType.VARCHAR, KdbTypes.timespan_type));

        //timestamp as string
        Assert.assertEquals("1970.01.02D00:00:00.001002003",
            KdbQueryStringBuilder.toLiteral("1970.01.02D00:00:00.001002003", MinorType.VARCHAR, KdbTypes.timestamp_type));
        Assert.assertEquals("0Np",
            KdbQueryStringBuilder.toLiteral(null                           , MinorType.VARCHAR, KdbTypes.timestamp_type));
        //timestamp as Timestamp
        Assert.assertEquals("2020.01.02D03:04:05.001000000",
            KdbQueryStringBuilder.toLiteral(new Timestamp(2020 - 1900, 0, 2, 3, 4, 5, 1000000), MinorType.VARCHAR, KdbTypes.timestamp_type));
    }
}
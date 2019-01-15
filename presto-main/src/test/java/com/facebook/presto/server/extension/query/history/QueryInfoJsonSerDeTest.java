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
package com.facebook.presto.server.extension.query.history;

import com.facebook.presto.execution.QueryInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;

import static org.testng.Assert.assertEquals;

public class QueryInfoJsonSerDeTest
{
    @Test
    public void testParseJson() throws IOException
    {
        ObjectMapper queryJsonParser = QueryHistorySQLStore.getQueryJsonParser();
        URL jsonResource = QueryInfoJsonSerDeTest.class.getClassLoader()
                .getResource("com/facebook/presto/server/extension/query/history/query-info.json");
        QueryInfo queryInfo = queryJsonParser.readValue(jsonResource, QueryInfo.class);
        assertEquals(queryInfo.getSession().getUser(), "s.fitoussi");
        assertEquals(queryInfo.getQueryStats().getCreateTime(),
                DateTime.parse("2019-01-15T10:56:03.086Z", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")));
    }
}

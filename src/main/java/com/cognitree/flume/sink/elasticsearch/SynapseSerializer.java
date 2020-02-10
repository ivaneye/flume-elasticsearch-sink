/*
 * Copyright 2017 Cognitree Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.cognitree.flume.sink.elasticsearch;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.elasticsearch.common.xcontent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * SimpleSerializer的修改版本，如果json中包含了recId，则作为_id处理
 */
public class SynapseSerializer implements Serializer {

    private static final Logger logger = LoggerFactory.getLogger(SynapseSerializer.class);

    private static Gson gson = new Gson();

    public XContentBuilder serialize(Event event) {
        XContentBuilder builder = null;
        try {
            String str = new String(event.getBody(), Charsets.UTF_8);
            Map map = gson.fromJson(str, Map.class);
            if (map.get("recId") != null) {
                Object recId = map.get("recId");
                map.put("_id", recId);
                str = gson.toJson(map);
            }
            logger.info("dataJson = [{}]", str);
            XContentParser parser = XContentFactory
                    .xContent(XContentType.JSON)
                    .createParser(NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            str);
            builder = jsonBuilder().copyCurrentStructure(parser);
            parser.close();
        } catch (IOException e) {
            logger.error("Error in Converting the body to json field " + e.getMessage(), e);
        }
        return builder;
    }

    @Override
    public void configure(Context context) {
        // No parameters needed from the configurations
    }
}

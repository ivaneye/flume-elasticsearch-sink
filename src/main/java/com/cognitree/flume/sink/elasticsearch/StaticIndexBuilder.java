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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * This class creates the index type and Id based on static fields
 */
public class StaticIndexBuilder implements IndexBuilder {

    private static final Logger logger = LoggerFactory.getLogger(StaticIndexBuilder.class);

    private String index;

    private String type;

    private String id;

    private Gson gson = new Gson();

    @Override
    public String getIndex(Event event) {
        String index;
        if (this.index != null) {
            index = this.index;
        } else {
            index = DEFAULT_ES_INDEX;
        }
        return index;
    }

    @Override
    public String getType(Event event) {
        String type;
        if (this.type != null) {
            type = this.type;
        } else {
            type = DEFAULT_ES_TYPE;
        }
        return type;
    }

    @Override
    public String getId(Event event) {
        // 判断event中是否有配置的id的值，如果有就作为主键
        if (this.id != null) {
            String str = new String(event.getBody(), Charsets.UTF_8);
            Map map = gson.fromJson(str, Map.class);
            if (map.get(this.id) != null) {
                return map.get(this.id) + "";
            }
        }
        return null;
    }

    @Override
    public void configure(Context context) {
        this.index = Util.getContextValue(context, ES_INDEX);
        this.type = Util.getContextValue(context, ES_TYPE);
        this.id = Util.getContextValue(context, ES_ID);
        logger.info("Simple Index builder, name [{}] type [{}] id [{}] ",
                new Object[]{this.index, this.type, this.id});

    }
}

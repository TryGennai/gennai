/**
 * Copyright 2013-2014 Recruit Technologies Co., Ltd. and contributors
 * (see CONTRIBUTORS.md)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  A copy of the
 * License is distributed with this work in the LICENSE.md file.  You may
 * also obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gennai.gungnir.ql.analysis;

import org.gennai.gungnir.ql.analysis.processor.ArgmentConvertException;
import org.gennai.gungnir.ql.analysis.processor.ArrayParameterConverter;
import org.gennai.gungnir.ql.analysis.processor.ConditionParameterConverter;
import org.gennai.gungnir.ql.analysis.processor.ListParameterConverter;
import org.gennai.gungnir.ql.analysis.processor.MapParameterConverter;
import org.gennai.gungnir.ql.analysis.processor.PrimitiveParameterConverter;
import org.gennai.gungnir.topology.processor.DummySpoutProcessor;
import org.gennai.gungnir.topology.processor.FileTtlCacheProcessor;
import org.gennai.gungnir.topology.processor.InMemorySpoutProcessor;
import org.gennai.gungnir.topology.processor.InMemoryTtlCacheProcessor;
import org.gennai.gungnir.topology.processor.JdbcFetchProcessor;
import org.gennai.gungnir.topology.processor.JdbcPersistProcessor;
import org.gennai.gungnir.topology.processor.KafkaEmitProcessor;
import org.gennai.gungnir.topology.processor.KafkaSpoutProcessor2;
import org.gennai.gungnir.topology.processor.LogAppendProcessor;
import org.gennai.gungnir.topology.processor.MongoFetchProcessor;
import org.gennai.gungnir.topology.processor.MongoPersistProcessor;
import org.gennai.gungnir.topology.processor.Processor;
import org.gennai.gungnir.topology.processor.WebEmitProcessor;
import org.gennai.gungnir.topology.processor.WebFetchProcessor;

public class ProcessorRegistry extends Registry {

  public ProcessorRegistry() throws RegisterException, ArgmentConvertException {
    addArgumentConverter(new PrimitiveParameterConverter());
    addArgumentConverter(new ArrayParameterConverter());
    addArgumentConverter(new ListParameterConverter());
    addArgumentConverter(new MapParameterConverter());
    addArgumentConverter(new ConditionParameterConverter());

    register("kafka_spout", KafkaSpoutProcessor2.class);
    register("memory_spout", InMemorySpoutProcessor.class);
    register("mongo_fetch", MongoFetchProcessor.class);
    register("web_fetch", WebFetchProcessor.class);
    register("jdbc_fetch", JdbcFetchProcessor.class);
    register("kafka_emit", KafkaEmitProcessor.class);
    register("mongo_persist", MongoPersistProcessor.class);
    register("web_emit", WebEmitProcessor.class);
    register("jdbc_persist", JdbcPersistProcessor.class);
    register("log", LogAppendProcessor.class);
    register("memory_cache", InMemoryTtlCacheProcessor.class);
    register("file_cache", FileTtlCacheProcessor.class);

    register("dummy_spout", DummySpoutProcessor.class);
  }

  public void registerProcessor(String name, Class<? extends Processor> registerClass)
      throws RegisterException, ArgmentConvertException {
    register(name, registerClass);
  }

  @Override
  public Processor create(String name, Object... args) throws RegisterException,
      ArgmentConvertException {
    return (Processor) super.create(name, args);
  }
}

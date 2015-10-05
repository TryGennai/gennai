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

package org.gennai.gungnir.ql;

import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.storm.guava.collect.Lists;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.ql.analysis.FileRegistry;
import org.gennai.gungnir.tuple.schema.FieldType;
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.ScalaConverters;

import com.google.common.collect.Sets;

public class FunctionEntity implements Serializable, Cloneable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public enum FunctionType {
    UDF, UDAF
  }

  public enum ScriptType {
    JAVASCRIPT("js", ".js"), PYTHON("python", ".py");

    private String shortName;
    private String extension;

    private ScriptType(String shortName, String extension) {
      this.shortName = shortName;
      this.extension = extension;
    }

    public String getShortName() {
      return shortName;
    }

    public String getExtension() {
      return extension;
    }
  }

  private String id;
  private String name;
  private FunctionType type;
  private String location;
  private transient List<String> topologies;
  private transient UserEntity owner;
  private Date createTime;
  private transient String comment;

  public FunctionEntity() {
  }

  private FunctionEntity(FunctionEntity c) {
    this.id = c.id;
    this.name = c.name;
    this.type = c.type;
    this.location = c.location;
    if (c.topologies != null) {
      this.topologies = Lists.newArrayList(c.topologies);
    }
    this.owner = c.owner.clone();
    this.createTime = c.createTime;
    this.comment = c.comment;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setType(FunctionType type) {
    this.type = type;
  }

  public FunctionType getType() {
    return type;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public String getLocation() {
    return location;
  }

  public void setTopologies(List<String> topologies) {
    this.topologies = topologies;
  }

  public List<String> getTopologies() {
    if (topologies == null) {
      topologies = Lists.newArrayList();
    }
    return topologies;
  }

  public void setOwner(UserEntity owner) {
    this.owner = owner;
  }

  public UserEntity getOwner() {
    return owner;
  }

  public void setCreateTime(Date createTime) {
    this.createTime = createTime;
  }

  public Date getCreateTime() {
    return createTime;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public String getComment() {
    return comment;
  }

  private static Set<Type> getTypes() {
    EnumSet<FieldType.TypeDef> typeNames = EnumSet.allOf(FieldType.TypeDef.class);
    Set<Type> types = Sets.newHashSet();
    for (FieldType.TypeDef typeName : typeNames) {
      types.add(typeName.getJavaType());
      if (typeName.getPrimitiveType() != null) {
        types.add(typeName.getPrimitiveType());
      }
    }
    Class<?>[] scalaTypes = ScalaConverters.getScalaTypes();
    for (int i = 0; i < scalaTypes.length; i++) {
      types.add(scalaTypes[i]);
    }
    types.add(Object.class);
    return types;
  }

  private List<List<Class<?>>> validateMethods(List<Method> methods, Set<Type> types)
      throws FunctionValidateException {
    List<List<Class<?>>> paramsList = Lists.newArrayList();
    for (Method method : methods) {
      if (method.isVarArgs()) {
        for (int i = 0; i < method.getParameterTypes().length - 1; i++) {
          if (!types.contains(method.getParameterTypes()[i])) {
            throw new FunctionValidateException(method.getParameterTypes()[i].getName()
                + " can't be used to the parameter type of " + method.getName());
          }
        }
        Class<?> paramType = method.getParameterTypes()[method.getParameterTypes().length - 1];
        if (!types.contains(paramType.getComponentType())) {
          throw new FunctionValidateException(paramType.getName()
              + " can't be used to the parameter type of " + method.getName());
        }
      } else {
        for (Class<?> paramType : method.getParameterTypes()) {
          if (!types.contains(paramType)) {
            throw new FunctionValidateException(paramType.getName()
                + " can't be used to the parameter type of " + method.getName());
          }
        }
      }

      if (!types.contains(method.getReturnType())) {
        throw new FunctionValidateException(method.getReturnType().getName()
            + " can't be used to the return type of " + method.getName());
      }

      paramsList.add(Lists.newArrayList(method.getParameterTypes()));
    }
    return paramsList;
  }

  private boolean matchesParams(List<Class<?>> params1, List<Class<?>> params2) {
    if (params1.size() != params2.size()) {
      return false;
    }

    /* TODO
    for (int i = 0; i < params1.size(); i++) {
      Class<?> type1 = params1.get(i);
      Class<?> type2 = params2.get(i);
      if (type1 != type2 && !type1.isAssignableFrom(type2) && !type2.isAssignableFrom(type1)) {
        return false;
      }
    }
    */

    return true;
  }

  private boolean matchesParamsList(List<List<Class<?>>> paramsList1,
      List<List<Class<?>>> paramsList2) {
    for (List<Class<?>> params1 : paramsList1) {
      boolean match = false;
      for (List<Class<?>> params2 : paramsList2) {
        if (matchesParams(params1, params2)) {
          match = true;
          break;
        }
      }
      if (!match) {
        return false;
      }
    }
    return true;
  }

  public void validate(FileRegistry fileRegistry) throws FunctionValidateException, IOException,
      MetaStoreException {
    fileRegistry.exportFiles(owner);
    ClassLoader classLoader = GungnirUtils.addToClassPath(Paths.get(fileRegistry.getCacheDir()));

    if (location.endsWith(".class")) {
      try {
        Class<?> functionClass = null;
        if (classLoader != null) {
          functionClass = Class.forName(location.substring(0, location.length() - 6), true,
              classLoader);
        } else {
          functionClass = Class.forName(location.substring(0, location.length() - 6));
        }

        List<Method> evaluateMethods = Lists.newArrayList();
        List<Method> excludeMethods = Lists.newArrayList();
        Method clearMethod = null;
        for (Method method : functionClass.getMethods()) {
          if ("evaluate".equals(method.getName())) {
            evaluateMethods.add(method);
          } else if ("exclude".equals(method.getName())) {
            excludeMethods.add(method);
          } else if ("clear".equals(method.getName())) {
            clearMethod = method;
          }
        }

        if (evaluateMethods.isEmpty()) {
          throw new FunctionValidateException("evaluate method is undefined");
        }
        Set<Type> types = getTypes();
        List<List<Class<?>>> evaluateParamsList = validateMethods(evaluateMethods, types);

        if (excludeMethods.isEmpty() && clearMethod == null) {
          type = FunctionType.UDF;
        } else {
          List<List<Class<?>>> excludeParamsList = validateMethods(excludeMethods, types);

          if (!matchesParamsList(evaluateParamsList, excludeParamsList)) {
            throw new FunctionValidateException("exclude method is undefined");
          }

          if (clearMethod != null) {
            if (clearMethod.getParameterTypes().length != 0) {
              throw new FunctionValidateException("clear method doesn't require parameters");
            }
            if (!void.class.equals(clearMethod.getReturnType())) {
              throw new FunctionValidateException("clear method doesn't require return");
            }
          } else {
            throw new FunctionValidateException("clear method is undefined");
          }

          type = FunctionType.UDAF;
        }
      } catch (ClassNotFoundException e) {
        throw new FunctionValidateException("'" + location + "' isn't registered");
      }
    } else {
      EnumSet<ScriptType> scriptTypes = EnumSet.allOf(ScriptType.class);

      ScriptType scriptType = null;
      for (ScriptType script : scriptTypes) {
        if (location.endsWith(script.extension)) {
          scriptType = script;
          break;
        }
      }

      if (scriptType == null) {
        throw new FunctionValidateException("'" + location + "' isn't registered");
      }

      InputStream is = null;
      if (classLoader != null) {
        is = classLoader.getResourceAsStream(location);
      } else {
        is = Thread.currentThread().getContextClassLoader().getResourceAsStream(location);
      }

      if (is == null) {
        throw new FunctionValidateException("'" + location + "' isn't registered");
      }

      InputStreamReader reader = new InputStreamReader(is);
      try {
        ScriptEngine engine = new ScriptEngineManager().getEngineByName(scriptType.shortName);
        engine.eval(reader);
        if (engine.get("evaluate") == null) {
          throw new FunctionValidateException("evaluate method is undefined");
        }
        type = FunctionType.UDF;

        if (engine.get("exclude") != null && engine.get("clear") != null) {
          type = FunctionType.UDAF;
        } else if (engine.get("exclude") == null && engine.get("clear") != null) {
          throw new FunctionValidateException("exclude method is undefined");
        } else if (engine.get("exclude") != null && engine.get("clear") == null) {
          throw new FunctionValidateException("clear method is undefined");
        }
      } catch (ScriptException e) {
        throw new FunctionValidateException(e.getMessage(), e);
      } finally {
        reader.close();
      }
    }
  }

  @Override
  public FunctionEntity clone() {
    return new FunctionEntity(this);
  }
}

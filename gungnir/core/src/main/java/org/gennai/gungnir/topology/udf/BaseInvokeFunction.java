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

package org.gennai.gungnir.topology.udf;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.ql.FunctionEntity;
import org.gennai.gungnir.tuple.schema.FieldType;
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.ScalaConverters;

import scala.reflect.ScalaSignature;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public abstract class BaseInvokeFunction extends BaseFunction<Object> implements UserDefined {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private FunctionEntity function;
  private transient String classPath;
  private transient Class<?> functionClass;
  private transient Map<Type, Type> primitiveTypesMap;

  protected BaseInvokeFunction(FunctionEntity function, String classPath) {
    this.function = function;
    this.classPath = classPath;
  }

  protected BaseInvokeFunction(BaseInvokeFunction c) {
    super(c);
    this.function = c.function;
    this.classPath = c.classPath;
  }

  @Override
  public FunctionEntity getFunction() {
    return function;
  }

  public String getClassPath() {
    if (classPath == null) {
      classPath = getConfig().getString(CLASS_PATH);
    }
    return classPath;
  }

  protected Class<?> getFunctionClass() throws IOException, ClassNotFoundException {
    if (functionClass == null) {
      String className = function.getLocation().substring(0, function.getLocation().length() - 6);
      if (getClassPath() != null) {
        ClassLoader classLoader = GungnirUtils.addToClassPath(Paths.get(getClassPath()));
        if (classLoader != null) {
          functionClass = Class.forName(className, true, classLoader);
        } else {
          functionClass = Class.forName(className);
        }
      } else {
        functionClass = Class.forName(className);
      }
    }
    return functionClass;
  }

  protected static final class InvokableMethod {

    private Method method;
    private boolean isScala;
    private Class<?>[] parameterTypes;

    private InvokableMethod(Method method) {
      this.method = method;

      if (method.getDeclaringClass().getAnnotation(ScalaSignature.class) != null) {
        isScala = true;
      }

      this.parameterTypes = new Class<?>[method.getParameterTypes().length];
      for (int i = 0; i < method.getParameterTypes().length; i++) {
        if (ScalaConverters.isScalaType(method.getParameterTypes()[i])) {
          parameterTypes[i] = ScalaConverters.asJavaType(method.getParameterTypes()[i]);
        } else {
          parameterTypes[i] = method.getParameterTypes()[i];
        }
      }
    }

    protected String getName() {
      return method.getName();
    }

    private boolean isVarArgs() {
      return method.isVarArgs();
    }

    private Class<?>[] getParameterTypes() {
      return parameterTypes;
    }

    protected Object invoke(Object obj, Object[] args) throws IllegalAccessException,
        IllegalArgumentException, InvocationTargetException {
      int paramLen = method.getParameterTypes().length;

      if (isScala) {
        if (method.isVarArgs()) {
          Class<?> compType = method.getParameterTypes()[paramLen - 1].getComponentType();
          Object varArgs = Array.newInstance(compType, args.length - paramLen + 1);
          for (int i = 0, j = paramLen - 1; i < Array.getLength(varArgs); i++, j++) {
            Array.set(varArgs, i, ScalaConverters.asScala(args[j]));
          }

          Object[] args2 = new Object[paramLen];
          for (int i = 0; i < args2.length - 1; i++) {
            args2[i] = ScalaConverters.asScala(args[i]);
          }
          args2[args2.length - 1] = varArgs;
          args = args2;
        } else {
          for (int i = 0; i < paramLen; i++) {
            args[i] = ScalaConverters.asScala(args[i]);
          }
        }
      } else {
        if (method.isVarArgs()) {
          Class<?> compType = method.getParameterTypes()[paramLen - 1].getComponentType();
          Object varArgs = Array.newInstance(compType, args.length - paramLen + 1);
          for (int i = 0, j = paramLen - 1; i < Array.getLength(varArgs); i++, j++) {
            Array.set(varArgs, i, args[j]);
          }

          Object[] args2 = new Object[paramLen];
          for (int i = 0; i < args2.length - 1; i++) {
            args2[i] = args[i];
          }
          args2[args2.length - 1] = varArgs;
          args = args2;
        }
      }

      Object ret = method.invoke(obj, args);

      if (isScala) {
        return ScalaConverters.asJava(ret);
      } else {
        return ret;
      }
    }
  }

  protected List<InvokableMethod> getMethods(Class<?> functionClass, String name) {
    List<InvokableMethod> invokableMethods = Lists.newArrayList();

    Method[] methods = functionClass.getMethods();
    for (Method method : methods) {
      if (method.getName().equals(name)
          && (method.getParameterTypes().length == getParameters().length
          || (method.isVarArgs()
          && method.getParameterTypes().length - 1 <= getParameters().length))) {
        invokableMethods.add(new InvokableMethod(method));
      }
    }

    return invokableMethods;
  }

  private Type getPrimitiveType(Type type) {
    if (primitiveTypesMap == null) {
      primitiveTypesMap = Maps.newHashMap();
      EnumSet<FieldType.TypeDef> typeNames = EnumSet.allOf(FieldType.TypeDef.class);
      for (FieldType.TypeDef typeName : typeNames) {
        if (typeName.getPrimitiveType() != null) {
          primitiveTypesMap.put(typeName.getJavaType(), typeName.getPrimitiveType());
        }
      }
    }
    return primitiveTypesMap.get(type);
  }

  private int matchParamType(Class<?> paramType, Object[] args, int index) {
    if (paramType.isArray()) {
      Class<?> argType = null;
      boolean isNull = true;
      for (int i = index; i < args.length; i++) {
        if (args[i] != null) {
          if (argType == null) {
            argType = args[i].getClass();
          } else if (args[i].getClass() != argType) {
            argType = Object.class;
          }
          isNull = false;
        }
      }

      Class<?> compType = paramType.getComponentType();
      if (isNull) {
        if (compType == Object.class) {
          return 2;
        } else {
          return 1;
        }
      } else {
        if (compType == Object.class) {
          return 1;
        } else {
          if (compType.isPrimitive()) {
            Type primitiveType = getPrimitiveType(argType);
            if (primitiveType != null && compType == primitiveType) {
              return 3;
            }
          } else {
            if (compType == argType || compType.isAssignableFrom(argType)) {
              return 4;
            }
          }
          return 0;
        }
      }
    } else {
      if (args[index] == null) {
        if (paramType == Object.class) {
          return 4;
        } else {
          return 3;
        }
      } else {
        if (paramType == Object.class) {
          return 2;
        } else {
          if (paramType.isPrimitive()) {
            Type primitiveType = getPrimitiveType(args[index].getClass());
            if (primitiveType != null && paramType == primitiveType) {
              return 5;
            }
          } else {
            if (paramType == args[index].getClass()
                || paramType.isAssignableFrom(args[index].getClass())) {
              return 6;
            }
          }
          return 0;
        }
      }
    }
  }

  protected List<InvokableMethod> findInvokeMethods(List<InvokableMethod> methods, Object[] args)
      throws IllegalAccessException, InvocationTargetException {
    List<InvokableMethod> invokeMethods = null;
    int maxPriority = 0;

    for (InvokableMethod method : methods) {
      int priority = 0;
      if (args.length == 0) {
        if (method.getParameterTypes().length == 0) {
          priority = 2;
        } else if (method.isVarArgs() && method.getParameterTypes().length == 1) {
          priority = 1;
        } else {
          priority = 0;
          break;
        }
      } else {
        for (int i = 0; i < method.getParameterTypes().length; i++) {
          int p = matchParamType(method.getParameterTypes()[i], args, i);
          if (p > 0) {
            priority += p;
          } else {
            priority = 0;
            break;
          }
        }
      }

      if (priority > 0) {
        if (priority > maxPriority) {
          maxPriority = priority;
          invokeMethods = Lists.newArrayList(method);
        } else if (priority == maxPriority) {
          invokeMethods.add(method);
        }
      }
    }

    return invokeMethods;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(function.getName());

    try {
      sb.append('(');
      if (getParameters() != null) {
        for (int i = 0; i < getParameters().length; i++) {
          if (i > 0) {
            sb.append(", ");
          }
          if (getParameters()[i].getClass().isArray()) {
            sb.append(Arrays.toString((Object[]) getParameters()[i]));
          } else {
            sb.append(getParameters()[i].toString());
          }
        }
      }
      sb.append(')');
      if (getAliasName() != null) {
        sb.append(" AS ");
        sb.append(getAliasName());
      }
    } catch (Exception e) {
      sb.append(e);
    }

    return sb.toString();
  }
}

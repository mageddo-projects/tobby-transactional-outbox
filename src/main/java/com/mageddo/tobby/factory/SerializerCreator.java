package com.mageddo.tobby.factory;

import java.lang.reflect.InvocationTargetException;

import com.mageddo.tobby.internal.utils.StringUtils;

import org.apache.kafka.common.serialization.Serializer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SerializerCreator {

  public static <T> Serializer<T> create(
      Class<? extends Serializer<T>> clazz, Class<? extends Serializer<T>> defaultClass
  ) {
    return create(clazz.getName(), defaultClass == null ? null : defaultClass.getName());
  }

  public static Serializer create(String clazz, String defaultClass) {
    if (StringUtils.isBlank(clazz)) {
      return (Serializer) newInstance(defaultClass);
    }
    return (Serializer) newInstance(clazz);
  }

  private static Object newInstance(String keySerializer) {
    try {
      return Class.forName(keySerializer)
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}

package com.mageddo.tobby.converter;

import com.mageddo.tobby.Header;
import com.mageddo.tobby.internal.utils.Base64;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class HeaderConverter {

  public static String encodeBase64(Header header) {
    return String.format("%s:%s", header.getKey(), Base64.encodeToString(header.getValue()));
  }

  public static Header decodeBase64(String encodedHeader) {
    final String[] tokens = encodedHeader.split(":");
    return Header.of(tokens[0], Base64.decode(tokens[1]));
  }
}

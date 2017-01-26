/**
 * Copyright 2016 Seznam a.s.
 *
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
package cz.seznam.euphoria.core.util;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * Util class for working with URI. It is part of euphoria-core - it is used only 
 * for internal usage and should not be used publicly.
 */
public class URIUtil {

  public static URI build(
      String scheme, String rawAuthority, String rawPath, 
      String rawQuery, String rawFragment)
  {
    return URI.create(
        new StringBuilder(128).append(scheme).append("://")
        .append(rawAuthority == null ? "" : rawAuthority)
        .append(rawPath == null ? "" : rawPath)
        .append(rawQuery == null ? "" : "?" + rawQuery)
        .append(rawFragment == null ? "" : "#" + rawFragment)
        .toString());
  }
  
  /**
   * @param s {@link String} to encode
   * @return encoded {@link String}
   */
  public static String paramEncode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // ~ thrown only if "UTF-8" is not supported; which must be by JDK-standards
      throw new IllegalStateException(e);
    }
  }

  /**
   * @param s {@link String} to decode
   * @return decoded {@link String}
   */
  public static String paramDecode(String s) {
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // ~ thrown only if "UTF-8" is not supported; which must be by JDK-standards
      throw new IllegalStateException(e);
    }
  }
}

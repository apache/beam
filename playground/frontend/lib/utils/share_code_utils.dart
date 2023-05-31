/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import 'dart:convert';

class ShareCodeUtils {
  static const _width = '90%';
  static const _height = '600px';

  /// The HTML of an <iframe> tag with the given [src] URL.
  static String iframe({
    required Uri src,
  }) {
    // Not URL-encoding because we expect only safe characters.
    return '<iframe'
        ' src="$src"'
        ' width="${_htmlEscape(_width)}"'
        ' height="${_htmlEscape(_height)}"'
        ' allow="clipboard-write" '
        '></iframe>';
  }

  static String _htmlEscape(String text) => const HtmlEscape().convert(text);
}

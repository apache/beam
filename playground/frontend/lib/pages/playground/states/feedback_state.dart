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

import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';

const kFeedbackKey = 'feedback';

class FeedbackState with ChangeNotifier {
  late SharedPreferences _pref;
  bool? isEnjoying;

  FeedbackState() {
    _initPrefs();
  }

  _initPrefs() async {
    _pref = await SharedPreferences.getInstance();
    isEnjoying = _pref.getBool(kFeedbackKey);
    notifyListeners();
  }

  setEnjoying(bool isEnjoying) {
    this.isEnjoying = isEnjoying;
    _pref.setBool(kFeedbackKey, isEnjoying);
    notifyListeners();
  }
}

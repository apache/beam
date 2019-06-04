/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

public class WordsAlphabet {

    private String alphabet;
    private String fruit;
    private String country;

    public WordsAlphabet(String alphabet, String fruit, String country) {
        this.alphabet = alphabet;
        this.fruit = fruit;
        this.country = country;
    }

    @Override
    public String toString() {
        return "WordsAlphabet{" +
                "alphabet='" + alphabet + '\'' +
                ", fruit='" + fruit + '\'' +
                ", country='" + country + '\'' +
                '}';
    }

}

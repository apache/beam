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
// 'Hello World!' program, just echos what was sent to it.

#include <iostream>
#include <fstream>

int main(int argc, char* argv[])
{
    if(argc < 3){
        std::cerr << "No parameter sent, must send the return file location and a statement to echo" << '\n';
        return 1;
    }
    std::string retFile = argv[1];
    std::string word = argv[2];
    std::ofstream myfile;
    myfile.open (retFile);
    myfile << "You again? Well ok, here is your word again." << word ;
    myfile.close();
    return 0;
}

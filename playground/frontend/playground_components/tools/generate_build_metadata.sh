#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

# Run this from the playground_components project root
FILE='lib/src/build_metadata.g.dart'

echo '// GENERATED CODE - DO NOT MODIFY BY HAND' > $FILE
echo '' >> $FILE

echo '// This file is generated during deployment to contain data about the commit.' >> $FILE
echo '// The copy of this file stored in the repository is for development purpose' >> $FILE
echo '// so the project can be started locally.' >> $FILE
echo '// It is safe to commit changes here everytime you run code generation.' >> $FILE

echo -n 'const buildCommitHash = ' >> $FILE
git rev-parse --sq HEAD >> $FILE
echo ';' >> $FILE

echo -n 'const buildCommitSecondsSinceEpoch = ' >> $FILE
git show -s --format=%ct HEAD >> $FILE
echo ';' >> $FILE

echo 'Written'
realpath $FILE
cat $FILE

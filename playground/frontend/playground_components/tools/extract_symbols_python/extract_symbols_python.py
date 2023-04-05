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

import argparse
import ast
import os
import yaml
from typing import Dict
from typing import List


def should_include_class_node(class_node: ast.ClassDef) -> bool:
    if class_node.name[0:1] == '_': return False
    if class_node.name[-4:] == 'Test': return False
    if class_node.name[-8:] == 'TestCase': return False
    return True


def should_include_function_node(member_node: ast.FunctionDef) -> bool:
    if member_node.name[0:1] == '_': return False
    return True


def should_include_property_node(name_node: ast.Name) -> bool:
    if name_node.id[0:1] == '_': return False
    return True


def get_file_symbols(file_name: str) -> Dict[str, Dict[str, List[str]]]:
    classes_dict = {}

    with open(file_name, 'r') as f:
        str = f.read()

    module = ast.parse(str)

    for class_node in module.body:
        if isinstance(class_node, ast.ClassDef):
            if not should_include_class_node(class_node): continue

            class_dict = {
                'methods': [],
                'properties': [],
            }

            for member_node in class_node.body:
                if isinstance(member_node, ast.FunctionDef):
                    if not should_include_function_node(member_node): continue
                    class_dict['methods'].append(member_node.name)

                elif isinstance(member_node, ast.Assign):
                    target = member_node.targets[0]
                    if isinstance(target, ast.Name):
                        if not should_include_property_node(target): continue
                        class_dict['properties'].append(target.id)

                elif isinstance(member_node, ast.AnnAssign):
                    target = member_node.target
                    if isinstance(target, ast.Name):
                        if not should_include_property_node(target): continue
                        class_dict['properties'].append(target.id)

            if len(class_dict['methods']) == 0:
                del class_dict['methods']
            else:
                class_dict['methods'].sort(key=lambda s: s.lower())

            if len(class_dict['properties']) == 0:
                del class_dict['properties']
            else:
                class_dict['properties'].sort(key=lambda s: s.lower())

            classes_dict[class_node.name] = class_dict

    return classes_dict


def get_dir_symbols_recursive(dir: str) -> Dict[str, Dict[str, List[str]]]:
    class_names = {}

    for root, subdirs, files in os.walk(dir):
        for file in files:
            if not file.endswith('.py'): continue

            file_path = os.path.join(root, file)
            class_names.update(get_file_symbols(file_path))

    return class_names


parser = argparse.ArgumentParser(description='Parses a directory with Python files and prints a YAML with symbols.')
parser.add_argument('dir', metavar='DIR', type=str, help='The directory to parse.')
args = parser.parse_args()

class_names = get_dir_symbols_recursive(args.dir)
class_names = dict(
    sorted(
        class_names.items(),
        key=lambda pair: pair[0].lower(),
    )
)

print(
    yaml.dump(
        class_names,
        default_flow_style=False,
        sort_keys=False,
    ),
    end='',
)

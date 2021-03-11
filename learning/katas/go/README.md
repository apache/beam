<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

### How to Setup
Please follow the below steps in order to setup the project properly:
* Using GoLand with the EduTools plugin, select "+ New Project"
* Click "Yes" to create project from existing sources when prompted
* Wait for indexing to finish
* Open the "Project" tool window, and select the "Course" view
* Your project is ready

For further instructions on how the GoLand Education works, you can refer
[here](https://www.jetbrains.com/help/education/educator-start-guide.html?section=Go).

### How to add a new course content
Since the Stepik course content follows specific rules, it is recommended to create content using the EduTools plugin.
Please follow the below steps in order to add new course content:
* Open IntelliJ/GoLand at the learning/katas/go directory and not root of the beam repository.
* Change the Project dropdown to Course.
* Right click where new content is to be created and select from the contextual menu
New lesson, section, or task, where appropriate.
* When the EduTools plugin creates new content, it will automatically create folders and files.  It is important to
note that it will not add the ASF license and should be done manually.
* When creating a new task, the EduTools plugin will create a go.mod in the folder.  It's important to delete this
as well as change the main folder it creates to cmd.  Please see other tasks in the Go SDK katas for examples
on the standard structure used throughout the course.
* It's also important to note that when updating content, the EduTools plugin will update files and also remove
the ASF license information.  When in doubt, the gradle rat task, at the root of the beam repository, can
uncover any files with missing licenses.

### Updating Stepik
To update a Stepik course, one must be assigned as an instructor.
* Open IntelliJ/GoLand at the learning/katas/go directory and not root of the beam repository.
* Change the Project dropdown to Course.
* Right click Beam Katas - Go (Course Creation) and find Course Creator in the context menu.
* Click "Update course on Stepik"
* The EduTools plugin will update and/or create `*-remote-info.yaml` files.  Commit any
of these changed or new files to the beam repository.

### Tracking the Go SDK Code Katas project

[BEAM-9676](https://issues.apache.org/jira/browse/BEAM-9676) tracks the Go SDK Code Katas project.

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

This is a task description file. Its content will be displayed to a learner in the **Task Description** window.

It supports both Markdown and HTML.
To toggle the format, you can rename **task.md** to **task.html**, or vice versa.
The default task description format can be changed in **Preferences | Tools | Education**, but this will not affect any existing task description files.

The following features are available in **task.md/task.html** which are specific to the EduTools plugin:

- Hints can be added anywhere in the task text. Type "hint" and press Tab. <div class="hint">Text of your hint</div>

- You can insert shortcuts in the task description.
While **task.html/task.md** is open, right-click anywhere on the **Editor** tab and choose the **Insert shortcut** option from the context menu.
For example: &shortcut:FileStructurePopup;.

- Insert the &percnt;`IDE_NAME`&percnt; macro, which will be replaced by the actual IDE name.
For example, **%IDE_NAME%**.

- Insert PSI elements, by using links like `<a href="psi_element://link.to.element">element description</a>`.
To get such a link, right-click the class or method and select **Copy Reference**. Then press &shortcut:EditorPaste; to insert the link where appropriate.
For example, a <a href="psi_element://java.lang.String#contains">link to the "contains" method</a>.
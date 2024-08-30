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

# Interactive Python Notebooks

Thank you for choosing to contribute an example! This will help both the Beam Developer community as well as the Beam User
community.

## Adding a notebook
1. Import an .ipynb file or create a new notebook on [Google Colab](https://colab.research.google.com).
2. In the upper-left corner menu, go to *File* -> *Save a copy in Github...*
    > You'll be asked to authenticate on GitHub.
3. Select the *Repository* as `your-username/beam` and the *Branch* you're working on, not master!
4. Set the file path to somewhere inside `examples/notebooks/`, e.g. `examples/notebooks/get-started/try-apache-beam-py.ipynb`.
    > You can leave the commit message as the default value, we'll squash all the commits into a single one at the end anyways.
5. Make sure the *Include a link to Colaboratory* is checked.
6. Pull the latest changes from the remote branch.
    ```sh
    git pull
    ```
    > If you have made local changes to the files, you can checkout them to discard the changes and pull the latest ones.
    ```sh
    git checkout -- file1 file2 .. fileN
    git pull
    ```
7. Repeat for all the notebooks you want to add.
8. From the project root directory, patch the Notebooks to point to the `master` branch instead of the local branch.
    ```sh
    python examples/notebooks/patch.py
    ```
9. Squash all the commits into a single commit.
    ```sh
    git commit --all --amend
    ```
    After this check the commit hash of the first (bottom) commit you want to include.
    ```sh
    git log
    ```
    Knowing the commit hash, we'll now rebase to that commit.
    ```sh
    git rebase -i <commit-hash>
    ```
    Your editor will open. Leave the first commit as `pick` and the rest replace them as `s` or `squash`. Don't worry about the commit messages for now.
    ```
    pick 55ed426e22 commit message 1
    s 0cbabbf704 commit message 2
    s d1513977fc commit message 3
    s 17c6db7950 commit message 4
    s 81634761e9 commit message 5
    ```
    > NOTE: in vim you can do this with `:2,$s/^pick/s/g`

    Finally, your editor will open again. All the commit messages will be visible, delete and reword as necessary to leave only one uncommented commit message. After closing your editor all your commits should be squashed :)

## Notebook Example Guidelines

1. Use [InteractiveRunner](https://cloud.google.com/dataflow/docs/guides/interactive-pipeline-development) and [DirectRunner](https://beam.apache.org/documentation/runners/direct/) as much as possible, to maintain Beam's vision of being runner-agnostic.
2. If you must use another runner or external technologies, document all commands and instructions needed for authentication, etc.
3. Document steps to import external files onto Colab, if needed.
4. Refrain from having too many code cells without explanation. Remember, this code should be able to be understood by someone with none or very limited experience in Beam!

*Thanks again for choosing to contribute an example!*

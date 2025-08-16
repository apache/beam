#!/bin/bash
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
# Utility script for tox.ini for running unit tests.
#
# Runs tests in parallel, except those not compatible with xdist. Combines
# exit statuses of runs, special-casing 5, which says that no tests were
# selected.
#
# $1 - suite base name
# $2 - additional arguments not parsed by tox (typically module names or
#   '-k keyword')
# $3 - optional arguments to pytest
#!/bin/bash

envname=${1?First argument required: suite base name}
posargs=$2
pytest_args=$3

# strip leading/trailing quotes from posargs because it can get double quoted as
# its passed through.
if [[ $posargs == '"'*'"' ]]; then
  # If wrapped in double quotes, remove them
  posargs="${posargs:1:${#posargs}-2}"
elif [[ $posargs == "'"*"'" ]]; then
  # If wrapped in single quotes, remove them.
  posargs="${posargs:1:${#posargs}-2}"
fi
echo "pytest_args: $pytest_args"
echo "posargs: $posargs"

# Define the regex for extracting the -m argument value
marker_regex="-m\s+('[^']+'|\"[^\"]+\"|\([^)]+\)|[^ ]+)"

# Initialize the user_marker variable.
user_marker=""

# Define regex pattern for quoted strings
quotes_regex="^[\"\'](.*)[\"\']$"

# Extract the user markers.
if [[ $posargs =~ $marker_regex ]]; then
  # Get the full match including -m and the marker.
  full_match="${BASH_REMATCH[0]}"

  # Get the marker with quotes (this is the first capture group).
  quoted_marker="${BASH_REMATCH[1]}"

  # Remove any quotes around the marker.
  if [[ $quoted_marker =~ $quotes_regex ]]; then
    user_marker="${BASH_REMATCH[1]}"
  else
    user_marker="$quoted_marker"
  fi

  # Remove the entire -m marker portion from posargs.
  posargs="${posargs/$full_match/}"
fi

# Combine user-provided marker with script's internal logic.
marker_for_parallel_tests="not no_xdist"
marker_for_sequential_tests="no_xdist"

if [[ -n $user_marker ]]; then
  # Combine user marker with internal markers.
  marker_for_parallel_tests="$user_marker and ($marker_for_parallel_tests)"
  marker_for_sequential_tests="$user_marker and ($marker_for_sequential_tests)"
fi

# Parse posargs to separate pytest options from test paths.
options=""
test_paths=""

# On Windows, convert backslashes to forward slashes
posargs=${posargs//\\//}

# Safely split the posargs string into individual arguments.
eval "set -- $posargs"

# Iterate through arguments.
while [[ $# -gt 0 ]]; do
  arg="$1"
  shift

  # If argument starts with dash, it's an option.
  if [[ "$arg" == -* ]]; then
    options+=" $arg"

    # Check if there's a next argument and it doesn't start with a dash.
    # This assumes it's a value for the current option.
    if [[ $# -gt 0 && "$1" != -* ]]; then
      # Get the next argument.
      next_arg="$1"

      # Check if it's quoted and remove quotes if needed.
      if [[ $next_arg =~ $quotes_regex ]]; then
        # Extract the content inside quotes.
        next_arg="${BASH_REMATCH[1]}"
      fi

      # Add the unquoted value to options.
      options+=" $next_arg"
      shift
    fi
  else
    # Otherwise it's a test path.
    test_paths+=" $arg"
  fi
done

# Construct the final pytest command arguments.
pyargs_section=""
if [[ -n "$test_paths" ]]; then
  pyargs_section="--pyargs $test_paths"
fi
pytest_command_args="$options $pyargs_section"

# Run tests in parallel.
echo "Running parallel tests with: pytest -m \"$marker_for_parallel_tests\" $pytest_command_args"
pytest -v -rs -o junit_suite_name=${envname} \
  --junitxml=pytest_${envname}.xml -m "$marker_for_parallel_tests" -n 6 --import-mode=importlib ${pytest_args} ${pytest_command_args}
status1=$?

# Run tests sequentially.
echo "Running sequential tests with: pytest -m \"$marker_for_sequential_tests\" $pytest_command_args"
pytest -v -rs -o junit_suite_name=${envname}_no_xdist \
  --junitxml=pytest_${envname}_no_xdist.xml -m "$marker_for_sequential_tests" --import-mode=importlib ${pytest_args} ${pytest_command_args}
status2=$?

# Exit with error if no tests were run in either suite (status code 5).
if [[ $status1 == 5 && $status2 == 5 ]]; then
  exit $status1
fi

# Exit with error if one of the statuses has an error that's not 5.
if [[ $status1 != 0 && $status1 != 5 ]]; then
  exit $status1
fi
if [[ $status2 != 0 && $status2 != 5 ]]; then
  exit $status2
fi
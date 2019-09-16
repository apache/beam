@rem ################################################################################
@rem #  Licensed to the Apache Software Foundation (ASF) under one
@rem #  or more contributor license agreements.  See the NOTICE file
@rem #  distributed with this work for additional information
@rem #  regarding copyright ownership.  The ASF licenses this file
@rem #  to you under the Apache License, Version 2.0 (the
@rem #  "License"); you may not use this file except in compliance
@rem #  with the License.  You may obtain a copy of the License at
@rem #
@rem #      http://www.apache.org/licenses/LICENSE-2.0
@rem #
@rem #  Unless required by applicable law or agreed to in writing, software
@rem #  distributed under the License is distributed on an "AS IS" BASIS,
@rem #  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem #  See the License for the specific language governing permissions and
@rem # limitations under the License.
@rem ################################################################################
@echo off

pushd %~dp0

set CMD_LINE_ARGS=%*
set ORG_CMD_LINE_ARGS=%*

for /F "tokens=1,2*" %%i in (project-mappings) do call :process %%i %%j

if not "%ORG_CMD_LINE_ARGS%" == "%CMD_LINE_ARGS%" (
  type deprecation-warning.txt

  echo Changed command to
  echo.
  echo   gradlew %CMD_LINE_ARGS%
  echo.
)

gradlew_orig.bat %CMD_LINE_ARGS% & popd
EXIT /B 0


:process
set VAR1=%1
set VAR2=%2
call set CMD_LINE_ARGS=%%CMD_LINE_ARGS:%VAR1%=%VAR2%%%
EXIT /B 0


// Licensed under the Apache License, Version 2.0 (the 'License'); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

/**
 * KernelCode namespace holds constant Python code that can be executed in an
 * IPython kernel that has Apache Beam Python SDK installed.
 */
export namespace KernelCode {
  export const COMMON_KERNEL_IMPORTS: string =
    'from apache_beam.runners.interactive' +
    ' import interactive_beam as ib\n' +
    'from apache_beam.runners.interactive' +
    ' import interactive_environment as ie\n';
}

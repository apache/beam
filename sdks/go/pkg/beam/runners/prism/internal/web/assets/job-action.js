/**
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

/** Element class for job action container. */
const JOB_ACTION = '.job-action'

/** Element class for cancel button. */
const CANCEL = '.cancel'

/** Element class assigned to RUNNING Job state. */
const RUNNING = 'RUNNING'

/**
 * Client for Job Management.
 *
 * Invokes backend for various Job Management tasks.
 */
const jobManager = {

    /**
     * Cancel a Job.
     * Invokes backend handler to request a Job cancellation state.
     * @param jobId
     * TODO(https://github.com/apache/beam/issues/29669) Send request to backend service.
     */
    cancel: function(jobId) {
        console.debug(`cancel button for Job: ${jobId} clicked`)
    }
}

/**
 * Encapsulates UI state such as enabling/disabling elements and querying various elements and their properties.
 */
const uiStateProvider = {

    /**
     * Queries the DOM for the Job Action container.
     * Logs an error if not found.
     * @returns {Element}
     */
    get jobAction() {
        let element = document.querySelector(JOB_ACTION)
        if (element === null) {
            console.error(`no element found at ${JOB_ACTION}`)
        }
        return element
    },

    /**
     * Queries Job Action container DOM for the cancel button.
     * Logs an error if not found.
     * @returns {Element}
     */
    get cancelButton() {
        let element = this.jobAction.querySelector(CANCEL)
        if (element === null) {
            console.error(`no element found at ${CANCEL} within ${this.jobAction}`)
        }
        return element
    },

    /**
     * Initializes the uiStateManager.
     * Called from the window's load event.
     */
    init() {
        this.cancelButton.disabled = this.isStateRunning === false
    },

    /**
     * Queries whether the Job Action container contains the RUNNING class name.
     * @returns {boolean}
     */
    get isStateRunning() {
        return this.jobAction.classList.contains(RUNNING)
    }
}

/**
 * Attaches an event listener to the window for 'load' events.
 */
window.addEventListener("load", function(){
    console.debug(JOB_ACTION, uiStateProvider.jobAction)
    console.debug(CANCEL, uiStateProvider.cancelButton)
    uiStateProvider.init()
})

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

/**
 * job-action.js provides UI functionality for taking actions on Prism Jobs via:
 * - jobManager:        Client for Job Management.
 * - uiStateProvider:   Encapsulates UI state of user interactive elements on the page.
 */

/** Element class for job action container. */
const JOB_ACTION = '.job-action'

/** Element class for cancel button. */
const CANCEL = '.cancel'

/** Element class assigned to RUNNING Job state. */
const RUNNING = 'RUNNING'

/** Element class for elements reporting Job state details. */
const JOB_STATE = '.job-state'

/** PATH holds consts that map to backend endpoints. */
const PATH = {

    /** ROOT_ is the Job management path prefix for mapped backend endpoints. */
    ROOT_: '/job',

    /** CANCEL maps to the backend endpoint to cancel a Job. Terminates with '/' to prevent ServeMux 301 redirect. */
    get CANCEL() {
        return `${this.ROOT_}/cancel/`
    }
}

/** HTTP related consts. */
const HTTP_POST = 'POST'

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
    cancel: function (jobId) {
        console.debug(`cancel button for Job: ${jobId} clicked`)
        const path = PATH.CANCEL
        const request = {
            method: HTTP_POST,
            body: JSON.stringify(new CancelJobRequest(jobId))
        }
        fetch(path, request)
            .then(response => {
                const requestJson = JSON.stringify(request)
                const responseJson = JSON.stringify(response)
                if (response.ok) {
                    console.debug(`Job cancellation request to ${path} of ${requestJson} for Job: ${jobId} sent successfully, response: ${responseJson}`)
                    uiStateProvider.onJobCancel(response)
                } else {
                    console.error(`Failed to send job cancellation request to ${path} of ${requestJson} for Job: ${jobId}, response: ${responseJson}`)
                }
            })
            .catch(error => {
                console.error(`Error occurred while sending job cancellation request for Job: ${jobId}`, error)
            })
    },
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
    },

    /**
     * Queries the element containing the {@link JOB_STATE} class.
     * @return {Element}
     */
    get jobStateElement() {
        let element = document.querySelector(JOB_STATE)
        if (element === null) {
            console.error(`no element found at ${JOB_STATE}`)
        }
        return element
    },

    /**
     * Callback for successful Job Cancel requests.
     * @param response {Response}
     */
    onJobCancel(response) {
        response.json().then(json => {
            console.debug(`job cancel response json: ${JSON.stringify(json)}`)
            uiStateProvider.jobStateElement.textContent = JobState_Enum[json.state]
        })
            .catch(error => {
                console.error(`error Response.json() ${error}`)
            })
    }
}

/**
 * Attaches an event listener to the window for 'load' events.
 */
window.addEventListener("load", function () {
    console.debug(JOB_ACTION, uiStateProvider.jobAction)
    console.debug(CANCEL, uiStateProvider.cancelButton)
    uiStateProvider.init()
})

/**
 * CancelJobRequest models a request to cancel a Job.
 *
 * Models after its proto namesake in:
 * https://github.com/apache/beam/blob/master/model/job-management/src/main/proto/org/apache/beam/model/job_management/v1/beam_job_api.proto
 */
class CancelJobRequest {
    job_id_;

    constructor(jobId) {
        this.job_id_ = jobId
    }

    /**
     * The ID of the Job to cancel.
     * @return {string}
     */
    get job_id() {
        return this.job_id_
    }

    /** toJSON overrides JSON.stringify serialization behavior. */
    toJSON() {
        return {job_id: this.job_id}
    }
}

/** Maps JobState_Enum from Job Management server response to the Job State name. See proto for more details:
 * https://github.com/apache/beam/blob/master/model/job-management/src/main/proto/org/apache/beam/model/job_management/v1/beam_job_api.proto
 */
const JobState_Enum = {
    0: "UNSPECIFIED",
    1: "STOPPED",
    2: "RUNNING",
    3: "DONE",
    4: "FAILED",
    5: "CANCELLED",
    6: "UPDATED",
    7: "DRAINING",
    8: "DRAINED",
    9: "STARTING",
    10: "CANCELLING",
    11: "UPDATING",
}

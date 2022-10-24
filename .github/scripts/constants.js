const DEFAULT_K = 5;

const MIN_RUNS = {
  "Build python source distribution and wheels": 10,
  10535798: 15,
  "PostCommit Go VR Flink": 2,
}; //Works with workflow name or id

const WORKFLOW_LABELS = {
  "Build python source distribution and wheels": "label1,label2",
  10535798: "label3,label3",
  "PostCommit Go VR Flink": "label4,label5",
};

const ISSUE_MANAGER_TAG = "ISSUE_MANAGER";

module.exports = { DEFAULT_K, MIN_RUNS, ISSUE_MANAGER_TAG, WORKFLOW_LABELS };

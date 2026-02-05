---
name: fix-ci
description: |
  Reviews the test failures for the current branch in Github Actions and fixes them

  Use this skill when the user wants to:
  - check CI status
  - fix CI failures

  Triggers: "fix ci", "fix the tests that are failing in ci", "see if CI is passing", "see what failed in CI"
---

Use the `gh` CLI to identify the most recent build for the current branch. Inspect which jobs failed, and then download and carefully analyze the logs for each failed job. Identify each failed test case or lint script or whatever, and create a todo for each. Then, fix each failure, validating each one as you go. Report back to the user with what was fixed and how.

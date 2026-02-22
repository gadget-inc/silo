---
name: fix-dst
description: |
  Reviews the latest DST simulation failures in Github Actions fixes them locally

  Use this skill when the user wants to fix remote DST failures discovered by CI DST runs
---

Use the `gh` CLI to identify the most recent build for the current branch of the `simulation-testing` github actions workflow. Retrieve the action logs and review the list of failed seeds at the end of the log. Identify the failed scenarios and reproduce the failure locally -- it should be fully deterministic and fail in the same way locally as it did in CI. Create a todo for each seed, and debug. Then, fix each failure, validating each one as you go. Report back to the user with what was fixed and how.

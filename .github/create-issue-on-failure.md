---
name: "Issue on workflow failure"
about: "Issue template for workflow failures"
title: 'Workflow "{{ env.WORKFLOW_NAME }}" failed'
labels: "build-failure"
---

The workflow "**{{ env.WORKFLOW_NAME }}**" failed. See logs:
https://github.com/{{ env.REPO_SLUG }}/actions/runs/{{ env.RUN_ID }}

Commit: https://github.com/{{ env.REPO_SLUG }}/commit/{{ tools.context.sha }}

// cc: {{ env.MENTION }} — please triage and resolve this issue

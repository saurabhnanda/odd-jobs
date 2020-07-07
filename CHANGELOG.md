# Changelog

## 0.2.2

- Bugfix in the `deleteJobQuery` function
- committed the cabal file to the repo

## 0.2.1

- build fixes

## 0.2.0

(broken build)

- Completed the web UI
- Added default functions for structured logging in JSON
- Introduced OddJobs.ConfigBuilder
- Introduced JobErrHandler machinery and changed the way onJobFailure works
- Made default job-timeout configurable via cfgDefaultJobTimeout
- Delete jobs when they are successful

## 0.1.0

- Initial release
- LISTEN/NOTIFY for immediate execution of jobs
- Polling for execution of jobs in the future, and for retrying failed jobs.
- Utilities for rapidly building CLIs that can be deployed as background daemons
- Structured logging
- Graceful shutdown
- Basic concurrency control
- Incomplete web UI
- First cut of Haddock docs
- Simple CLI example

=== 1. Create a table to store jobs

In this example, our jobs table will be called `jobs_test`

<div class="lhs-code">
```
ghci> import Datasbe.PostgreSQL.Simple (connectPostgreSQL)
ghci> import OddJobs.Migrations
ghci> conn <- connectPostgreSQL "dbname=jobs_test user=jobs_test password=jobs_test host=localhost"
ghci> createJobTable conn "jobs_test"
```
</div>

=== 2. Create a module for your job-runner

Ideally, this module should be compiled into a separate executable and should depend on your application's library module. Please refer to TODO for an example of how to setup your `package.yaml` (or cabal) file.

| If you do not wish to deploy odd-jobs as an independent executable, you may embed it within your main application's executable as well. This is described in TODO.

\begin{code}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

module OddJobsCliExample where

import OddJobs.Job (Job(..),  ConcurrencyControl(..), Config(..), throwParsePayload)
import OddJobs.ConfigBuilder (mkConfig, withConnectionPool, defaultTimedLogger, defaultLogStr, defaultJobToText, defaultJobType)
import OddJobs.Cli (defaultMain)

-- Note: It is not necessary to use fast-logger. You can use any logging library
-- that can give you a logging function in the IO monad.
import System.Log.FastLogger(withTimedFastLogger, LogType'(..), defaultBufSize)
import System.Log.FastLogger.Date (newTimeCache, simpleTimeFormat)

import Data.Text (Text)
import Data.Aeson as Aeson
import GHC.Generics

-- This example is using these functions to introduce an artificial delay of a
-- few seconds in one of the jobs. Otherwise it is not really needed.
import OddJobs.Types (delaySeconds, Seconds(..))
\end{code} 

=== 3. Set-up a Haskell type to represent your job-payload

- Ideally, this data-type should be defined _inside_ your application's code and the module containing this type-definition should be part of the `exposed-modules` stanza. Again, please look at TODO for an example of how to setup your `package.yaml` (or cabal) file.
- To work with all the default settings provided by 'OddJobs.ConfigBuilder' this data-type should have a **"tagged" JSON serialisation,** i.e.:

    ```json
    {"tag": "SendWelcomEmail", "contents": 10}
    ```

  In case your JSON payload does not conform to this structure, please look at TODO.

- In this example, we are _blindly_ deriving `ToJSON` and `FromJSON` instances because the default behaviour of Aeson is to generate a tagged JSON as-per the example given above.

\begin{code}
data MyJob
  = SendWelcomeEmail Int
  | SendPasswordResetEmail Text
  | SetupSampleData Int
  deriving (Eq, Show, Generic, ToJSON, FromJSON)
\end{code}

=== 4. Write the core job-runner function

In this example, the core job-runner function is in the `IO` monad. In all probability, you application's code will be in a custom monad, and not IO. Pleae refer to TODO, on how to work with custom monads.

\begin{code}
myJobRunner :: Job -> IO ()
myJobRunner job = do
  (throwParsePayload job) >>= \case
    SendWelcomeEmail userId -> do
      putStrLn $ "This should call the function that actually sends the welcome email. " <>
        "\nWe are purposely waiting 60 seconds before completing this job so that graceful shutdown can be demonstrated."
      delaySeconds (Seconds 60)
      putStrLn "60 second wait is now over..."
    SendPasswordResetEmail tkn ->
      putStrLn "This should call the function that actually sends the password-reset email"
    SetupSampleData userId -> do
      Prelude.error "User onboarding is incomplete"
      putStrLn "This should call the function that actually sets up sample data in a newly registered user's account"
\end{code}

=== 5. Write the main function using `OddJobs.Cli`

\begin{code}
main :: IO ()
main = do
  defaultMain startJobMonitor
  where
    -- A callback-within-callback function. If the commands-line args contain a
    -- `start` command, this function will be called. Once this function has
    -- constructed the 'Config' (which requires setting up a logging function,
    -- and a DB pool) it needs to execute the `callback` function that is passed
    -- to it.
    startJobMonitor callback =

      -- a utility function provided by `OddJobs.ConfigBuilder` which ensures
      -- that the DB pool is gracefully destroyed upon shutdown.
      withConnectionPool (Left "dbname=jobs_test user=jobs_test password=jobs_test host=localhost")$ \dbPool -> do

        -- Boilerplate code to setup a TimedFastLogger (from the fast-logger library)
        tcache <- newTimeCache simpleTimeFormat
        withTimedFastLogger tcache (LogFileNoRotate "oddjobs.log" defaultBufSize) $ \logger -> do

          -- Using the default string-based logging provided by
          -- `OddJobs.ConfigBuilder`. If you want to actually use
          -- structured-logging you'll need to define your own logging function.
          let jobLogger = defaultTimedLogger logger (defaultLogStr (defaultJobToText defaultJobType))
              cfg = mkConfig jobLogger "jobs" dbPool (MaxConcurrentJobs 50) myJobRunner Prelude.id

          -- Finally, executing the callback function that was passed to me...
          callback cfg
\end{code}

=== 6. Compile and start the Odd Jobs runner

<div class="lhs-code">
```
$ stack install <your-package-name>:exe:odd-jobs-cli
$ odd-jobs-cli start --daemonize --web-ui-basic-auth=oddjobs --web-ui-basic-password=awesome
```
</div>


=== 7. Enqueue some jobs from within your application's code

<div class="lhs-code">
```
ghci> import OddJobs.Job (createJob)
ghci> import Database.PostgreSQL.Simple
ghci> conn <- connectPostgreSQL "dbname=jobs_test user=jobs_test password=jobs_test host=localhost"
ghci> createJob conn $ SendWelcomeEmail 10
ghci> createJob conn $ SetupSampleData 10
```
</div>

=== 8. Check-out the awesome web UI

Visit [http://localhost:7777](http://localhost:7777) (`username=oddjobs` / `password=awesome` as configured earlier).

=== 9. Check-out the log file to see what Odd Jobs is doing

<div class="lhs-code">
```
$ tail -f oddjobs.log
```
</div>

=== 10. Finally, shutdown Odd Jobs _gracefully_

Please read [graceful shutdown](#graceful-shutdown) to know more.

<div class="lhs-code">
```
$ odd-jobs-cli stop --timeout 65
```
</div>

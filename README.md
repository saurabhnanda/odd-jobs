# Introduction

Please read the **detailed tutorial** available at [Odd Jobs Tutorial](https://www.haskelltutorials.com/odd-jobs). Broadly, here is the **most common** way to get started with this library:

- (For a working version of the steps described below, please check the [simple-example](https://github.com/saurabhnanda/odd-jobs/tree/master/simple-example) directory in the project repo).
- Create a DB table to hold your jobs using the `OddJobs.Migrations.createJobTable` function.
- Create a sum-type to represent your job payloads, for example:
    ```
    data MyJob = SendWelcomeEmail Int
               | SendPasswordResetEmail Text
               | SetupSampleData Int
               deriving (Eq, Show, Generic, ToJSON, FromJSON)
    ```
- Ensure that you use a "tagged" JSON encoding for this data-type for best results (this is the default behaviour in Aeson). For example:
    ```
    Aeson.encode (SendWelcomeEmail 10) == "{\"tag\":\"SendWelcomeEmail\", \"contents\":10}"
    ```
    
- Create you "core" job-runner function:
    ```
    myJobRunner :: Job -> IO ()
    myJobRunner Job{jobPayload} = 
      case jobPayload of
        SendWelcomeEmail userId -> sendWelcomeEmail userId
        SendPasswordResetEmail tkn -> sendPasswordResetEmail tkn
        SetupSampleData userId -> sendPasswordResetEmail userId
    ```
    
- Use `OddJobs.Job.createJob` and `OddJobs.Job.scheduleJob` within your app whenever you want to enqueue a job.
- Use `OddJobs.Cli` to wrap all of this together into a nice CLI that can fork itself as a background daemon.
- Hook this up to your [deployment scripts](https://www.haskelltutorials.com/odd-jobs/deployment.html) and you're good to go!

module CliParser where

import OddJobs.Cli

main :: IO ()
main = parseCliArgs >>= (pure . show) >>= putStrLn

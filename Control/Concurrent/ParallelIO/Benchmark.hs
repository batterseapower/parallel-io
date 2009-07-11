module Main where

import Data.IORef
import Data.Time.Clock

import Control.Concurrent.ParallelIO.Global


n :: Int
n = 1000000

main :: IO ()
main = do
    r <- newIORef (0 :: Int)
    let incRef = atomicModifyIORef r (\a -> (a, a))
    time $ parallel_ $ replicate n $ incRef
    v <- readIORef r
    stopGlobalPool
    print v

time :: IO a -> IO a
time action = do
    start <- getCurrentTime
    result <- action
    stop <- getCurrentTime
    print $ stop `diffUTCTime` start
    return result
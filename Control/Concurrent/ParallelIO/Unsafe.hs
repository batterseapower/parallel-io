module Control.Concurrent.ParallelIO.Unsafe (
    parallel_,
    stopGlobalPool
  ) where

import Control.Concurrent
import Control.Monad

import GHC.Conc

import System.IO.Unsafe

import Control.Concurrent.ParallelIO.Safe


{-# NOINLINE globalPool #-}
globalPool :: Pool
globalPool = unsafePerformIO $ startPool numCapabilities

stopGlobalPool :: IO ()
stopGlobalPool = stopPool globalPool


-- | Run the list of computations in parallel
--   Rule: No thread should get pre-empted (although not a guarantee)
--         On return all actions have been performed
parallel_ :: [IO a] -> IO ()
parallel_ xs | numCapabilities <= 1 = sequence_ xs
parallel_ [] = return ()
parallel_ [x] = x >> return ()
parallel_ (x1:xs) = do
    count <- newMVar $ length xs
    pause <- newEmptyMVar
    forM_ xs $ \x ->
        enqueueOnPool globalPool $ do
            x
            modifyMVar count $ \i -> do
                let i2 = i - 1
                    kill = i2 == 0
                when kill $ putMVar pause ()
                return (i2, kill)
    x1
    spawnPoolWorkerFor globalPool
    takeMVar pause

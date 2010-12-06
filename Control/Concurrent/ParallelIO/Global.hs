-- | Parallelism combinators with an implicit global thread-pool.
--
-- The most basic example of usage is:
--
-- > main = parallel_ [putStrLn "Echo", putStrLn " in parallel"] >> stopGlobalPool
--
-- Make sure that you compile with @-threaded@ and supply @+RTS -N2 -RTS@
-- to  the generated Haskell executable, or you won't get any parallelism.
--
-- The "Control.Concurrent.ParallelIO.Local" module provides a more general
-- interface which allows explicit passing of pools and control of their size.
-- This module is implemented on top of that one by maintaining a shared global thread
-- pool with one thread per capability.
module Control.Concurrent.ParallelIO.Global (
    globalPool, stopGlobalPool,
    extraWorkerWhileBlocked, spawnPoolWorker, killPoolWorker,
    
    parallel_, parallel, parallelInterleaved
  ) where

import GHC.Conc

import System.IO.Unsafe

import qualified Control.Concurrent.ParallelIO.Local as L


{-# NOINLINE globalPool #-}
globalPool :: L.Pool
globalPool = unsafePerformIO $ L.startPool numCapabilities

-- | In order to reliably make use of the global parallelism combinators,
-- you must invoke this function after all calls to those combinators have
-- finished. A good choice might be at the end of 'main'.
--
-- See also 'L.stopPool'.
stopGlobalPool :: IO ()
stopGlobalPool = L.stopPool globalPool

-- | Wrap any IO action used from your worker threads that may block with this method:
-- it temporarily spawns another worker thread to make up for the loss of the old blocked
-- worker.
--
-- See also 'L.extraWorkerWhileBlocked'.
extraWorkerWhileBlocked :: IO a -> IO a
extraWorkerWhileBlocked = L.extraWorkerWhileBlocked globalPool

-- | Internal method for adding extra unblocked threads to a pool if one is going to be
-- temporarily blocked.
--
-- See also 'L.spawnPoolWorkerFor'.
spawnPoolWorker :: IO ()
spawnPoolWorker = L.spawnPoolWorkerFor globalPool

-- | Internal method for removing threads from a pool after we become unblocked.
--
-- See also 'L.killPoolWorkerFor'.
killPoolWorker :: IO ()
killPoolWorker = L.killPoolWorkerFor globalPool

-- | Execute the given actions in parallel on the global thread pool.
--
-- See also 'L.parallel_'.
parallel_ :: [IO a] -> IO ()
parallel_ = L.parallel_ globalPool

-- | Execute the given actions in parallel on the global thread pool,
-- returning the results in the same order as the corresponding actions.
--
-- See also 'L.parallel'.
parallel :: [IO a] -> IO [a]
parallel = L.parallel globalPool

-- | Execute the given actions in parallel on the global thread pool,
-- returning the results in the approximate order of completion.
--
-- See also 'L.parallelInterleaved'.
parallelInterleaved :: [IO a] -> IO [a]
parallelInterleaved = L.parallelInterleaved globalPool

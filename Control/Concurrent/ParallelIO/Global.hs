-- | Parallelism combinators with an implicit global thread-pool.
--
-- The most basic example of usage is:
--
-- > main = parallel_ [putStrLn "Echo", putStrLn " in parallel"] >> stopGlobalPool
--
-- Make sure that you compile with @-threaded@ and supply @+RTS -N2 -RTS@
-- to  the generated Haskell executable, or you won't get any parallelism.
--
-- If you plan to allow your worker items to block, then you should read the documentation for 'extraWorkerWhileBlocked'.
--
-- The "Control.Concurrent.ParallelIO.Local" module provides a more general
-- interface which allows explicit passing of pools and control of their size.
-- This module is implemented on top of that one by maintaining a shared global thread
-- pool with one thread per capability.
module Control.Concurrent.ParallelIO.Global (
    -- * Executing actions
    parallel_, parallelE_, parallel, parallelE,
    parallelInterleaved, parallelInterleavedE,
    parallelFirst, parallelFirstE,

    -- * Global pool management
    globalPool, stopGlobalPool,
    extraWorkerWhileBlocked,
    
    -- * Advanced global pool management
    spawnPoolWorker, killPoolWorker
  ) where

import GHC.Conc

import Control.Exception

import System.IO.Unsafe

import qualified Control.Concurrent.ParallelIO.Local as L

-- | The global thread pool. Contains as many threads as there are capabilities.
--
-- Users of the global pool must call 'stopGlobalPool' from the main thread at the end of their program.
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
 -- TODO: could I lift the requirement to call this function with a touchPool function after the parallel combinators?

-- | Wrap any IO action used from your worker threads that may block with this method:
-- it temporarily spawns another worker thread to make up for the loss of the old blocked
-- worker.
--
-- See also 'L.extraWorkerWhileBlocked'.
extraWorkerWhileBlocked :: IO a -> IO a
extraWorkerWhileBlocked = L.extraWorkerWhileBlocked globalPool

-- | Internal method for adding extra unblocked threads to a pool if one of the current
-- worker threads is going to be temporarily blocked. Unrestricted use of this is unsafe,
-- so we reccomend that you use the 'extraWorkerWhileBlocked' function instead if possible.
--
-- See also 'L.spawnPoolWorkerFor'.
spawnPoolWorker :: IO ()
spawnPoolWorker = L.spawnPoolWorkerFor globalPool

-- | Internal method for removing threads from a pool after one of the threads on the pool
-- becomes newly unblocked. Unrestricted use of this is unsafe, so we reccomend that you use
-- the 'extraWorkerWhileBlocked' function instead if possible.
--
-- See also 'L.killPoolWorkerFor'.
killPoolWorker :: IO ()
killPoolWorker = L.killPoolWorkerFor globalPool

-- | Execute the given actions in parallel on the global thread pool.
--
-- Users of the global pool must call 'stopGlobalPool' from the main thread at the end of their program.
--
-- See also 'L.parallel_'.
parallel_ :: [IO a] -> IO ()
parallel_ = L.parallel_ globalPool

-- | Execute the given actions in parallel on the global thread pool, reporting exceptions explicitly.
--
-- Users of the global pool must call 'stopGlobalPool' from the main thread at the end of their program.
--
-- See also 'L.parallelE_'.
parallelE_ :: [IO a] -> IO [Maybe SomeException]
parallelE_ = L.parallelE_ globalPool

-- | Execute the given actions in parallel on the global thread pool,
-- returning the results in the same order as the corresponding actions.
--
-- Users of the global pool must call 'stopGlobalPool' from the main thread at the end of their program.
--
-- See also 'L.parallel'.
parallel :: [IO a] -> IO [a]
parallel = L.parallel globalPool

-- | Execute the given actions in parallel on the global thread pool,
-- returning the results in the same order as the corresponding actions and reporting exceptions explicitly.
--
-- Users of the global pool must call 'stopGlobalPool' from the main thread at the end of their program.
--
-- See also 'L.parallelE'.
parallelE :: [IO a] -> IO [Either SomeException a]
parallelE = L.parallelE globalPool

-- | Execute the given actions in parallel on the global thread pool,
-- returning the results in the approximate order of completion.
--
-- Users of the global pool must call 'stopGlobalPool' from the main thread at the end of their program.
--
-- See also 'L.parallelInterleaved'.
parallelInterleaved :: [IO a] -> IO [a]
parallelInterleaved = L.parallelInterleaved globalPool

-- | Execute the given actions in parallel on the global thread pool,
-- returning the results in the approximate order of completion and reporting exceptions explicitly.
--
-- Users of the global pool must call 'stopGlobalPool' from the main thread at the end of their program.
--
-- See also 'L.parallelInterleavedE'.
parallelInterleavedE :: [IO a] -> IO [Either SomeException a]
parallelInterleavedE = L.parallelInterleavedE globalPool

-- | Run the list of computations in parallel, returning the result of the first
-- thread that completes with (Just x), if any.
--
-- Users of the global pool must call 'stopGlobalPool' from the main thread at the end of their program.
--
-- See also 'L.parallelFirst'.
parallelFirst :: [IO (Maybe a)] -> IO (Maybe a)
parallelFirst = L.parallelFirst globalPool

-- | Run the list of computations in parallel, returning the result of the first
-- thread that completes with (Just x), if any, and reporting any exception explicitly.
--
-- Users of the global pool must call 'stopGlobalPool' from the main thread at the end of their program.
--
-- See also 'L.parallelFirstE'.
parallelFirstE :: [IO (Maybe a)] -> IO (Maybe (Either SomeException a))
parallelFirstE = L.parallelFirstE globalPool
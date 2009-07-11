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
    stopGlobalPool,
    
    parallel_
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

-- | Execute the given actions in parallel on the global thread pool.
--
-- See also 'L.parallel_'.
parallel_ :: [IO a] -> IO ()
parallel_ = L.parallel_ globalPool

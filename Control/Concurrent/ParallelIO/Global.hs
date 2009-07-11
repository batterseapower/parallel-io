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

stopGlobalPool :: IO ()
stopGlobalPool = L.stopPool globalPool

parallel_ :: [IO a] -> IO ()
parallel_ = L.parallel_ globalPool

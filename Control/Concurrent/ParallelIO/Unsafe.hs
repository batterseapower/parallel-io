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


-- | Run the list of computations in parallel.
--
-- Has the following properties:
-- 1) Never creates more unblocked threads than are specified to live in
--    the pool. NB: this count includes the thread executing parallel_.
--    This should minimize contention and hence pre-emption.
-- 2) On return all actions have been performed.
-- 3) The above properties are true even if parallel_ is used by an
--    action which is itself being executed by parallel_.
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

-- An alternative implementation of parallel_ might:
--  1) Avoid spawning an additional thread
--  2) Remove the need for the pause mvar
--
-- By having the thread invoking parallel_ also pull stuff from the
-- work pool, and poll the count variable after every item to see
-- if everything has been processed (which would cause it to stop
-- processing work pool items). However:
--  1) This is less timely, because the main thread might get stuck
--     processing a big work item not related to the current parallel_
--     invocation, and wouldn't poll (and return) until that was done.
--  2) It actually performs a bit less well too - or at least it did on
--     my benchmark with lots of cheap actions, where polling would
--     be relatively frequent. Went from 8.8s to 9.1s.
--
-- For posterity, the implementation was:
--
-- parallel_ :: [IO a] -> IO ()
-- parallel_ xs | numCapabilities <= 1 = sequence_ xs
-- parallel_ [] = return ()
-- parallel_ [x] = x >> return ()
-- parallel_ (x1:xs) = do
--     count <- newMVar $ length xs
--     forM_ xs $ \x ->
--         enqueueOnPool globalPool $ do
--             x
--             modifyMVar_ count $ \i -> return (i - 1)
--             return False
--     x1
--     done <- fmap (== 0) $ readMVar count
--     unless done $ myWorkerLoop globalPool count
-- 
-- myWorkerLoop :: Pool -> MVar Int -> IO ()
-- myWorkerLoop pool count = do
--     kill <- join $ readChan (pool_queue pool)
--     done <- fmap (== 0) $ readMVar count
--     unless (kill || done) (myWorkerLoop pool count)
--
-- NB: in this scheme, kill is only True when the program is exiting.

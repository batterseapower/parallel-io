{-# LANGUAGE ScopedTypeVariables #-}
-- | Parallelism combinators with explicit thread-pool creation and
-- passing.
--
-- The most basic example of usage is:
--
-- > main = withPool 2 $ \pool -> parallel_ pool [putStrLn "Echo", putStrLn " in parallel"]
--
-- Make sure that you compile with @-threaded@ and supply @+RTS -N2 -RTS@
-- to  the generated Haskell executable, or you won't get any parallelism.
--
-- If you plan to allow your worker items to block, then you should read the documentation for 'extraWorkerWhileBlocked'.
--
-- The "Control.Concurrent.ParallelIO.Global" module is implemented
-- on top of this one by maintaining a shared global thread pool
-- with one thread per capability.
module Control.Concurrent.ParallelIO.Local (
    -- * Executing actions
    parallel_, parallelE_, parallel, parallelE, parallelInterleaved, parallelInterleavedE,

    -- * Pool management
    Pool, withPool, startPool, stopPool,
    extraWorkerWhileBlocked,
    
    -- * Advanced pool management
    spawnPoolWorkerFor, killPoolWorkerFor
  ) where

import Control.Concurrent.ParallelIO.Compat

import Control.Concurrent
import Control.Exception
import Control.Monad

import System.IO

import Prelude hiding (catch)


-- | A thread pool, containing a maximum number of threads. The best way to
-- construct one of these is using 'withPool'.
data Pool = Pool {
    pool_threadcount :: Int,
    pool_sem :: QSem
  }

-- | A slightly unsafe way to construct a pool. Make a pool from the maximum
-- number of threads you wish it to execute (including the main thread
-- in the count).
-- 
-- If you use this variant then ensure that you insert a call to 'stopPool'
-- somewhere in your program after all users of that pool have finished.
--
-- A better alternative is to see if you can use the 'withPool' variant.
startPool :: Int -> IO Pool
startPool threadcount
  | threadcount < 1 = error $ "startPool: thread count must be strictly positive (was " ++ show threadcount ++ ")"
  | otherwise = fmap (Pool threadcount) $ newQSem (threadcount - 1)

-- | Clean up a thread pool. If you don't call this from the main thread then no one holds the queue,
-- the queue gets GC'd, the threads find themselves blocked indefinitely, and you get exceptions.
-- 
-- This cleanly shuts down the threads so the queue isn't important and you don't get
-- exceptions.
--
-- Only call this /after/ all users of the pool have completed, or your program may
-- block indefinitely.
stopPool :: Pool -> IO ()
stopPool pool = replicateM_ (pool_threadcount pool - 1) $ killPoolWorkerFor pool

-- | A safe wrapper around 'startPool' and 'stopPool'. Executes an 'IO' action using a newly-created
-- pool with the specified number of threads and cleans it up at the end.
withPool :: Int -> (Pool -> IO a) -> IO a
withPool threadcount = bracket (startPool threadcount) stopPool


-- | You should wrap any IO action used from your worker threads that may block with this method.
-- It temporarily spawns another worker thread to make up for the loss of the old blocked
-- worker.
--
-- This is particularly important if the unblocking is dependent on worker threads actually doing
-- work. If you have this situation, and you don't use this method to wrap blocking actions, then
-- you may get a deadlock if all your worker threads get blocked on work that they assume will be
-- done by other worker threads.
--
-- An example where something goes wrong if you don't use this to wrap blocking actions is the following example:
--
-- > newEmptyMVar >>= \mvar -> parallel_ pool [readMVar mvar, putMVar mvar ()]
--
-- If we only have one thread, we will sometimes get a schedule where the 'readMVar' action is run
-- before the 'putMVar'. Unless we wrap the read with 'extraWorkerWhileBlocked', if the pool has a
-- single thread our program to deadlock, because the worker will become blocked and no other thread
-- will be available to execute the 'putMVar'.
--
-- The correct code is:
--
-- > newEmptyMVar >>= \mvar -> parallel_ pool [extraWorkerWhileBlocked pool (readMVar mvar), putMVar mvar ()]
extraWorkerWhileBlocked :: Pool -> IO a -> IO a
extraWorkerWhileBlocked pool = bracket_ (spawnPoolWorkerFor pool) (killPoolWorkerFor pool)

-- | Internal method for adding extra unblocked threads to a pool if one of the current
-- worker threads is going to be temporarily blocked. Unrestricted use of this is unsafe,
-- so we recommend that you use the 'extraWorkerWhileBlocked' function instead if possible.
spawnPoolWorkerFor :: Pool -> IO ()
spawnPoolWorkerFor pool = signalQSem (pool_sem pool)

-- | Internal method for removing threads from a pool after one of the threads on the pool
-- becomes newly unblocked. Unrestricted use of this is unsafe, so we reccomend that you use
-- the 'extraWorkerWhileBlocked' function instead if possible.
killPoolWorkerFor :: Pool -> IO ()
killPoolWorkerFor pool = waitQSem (pool_sem pool)


-- | Run the list of computations in parallel.
--
-- Has the following properties:
--
--  1. Never creates more or less unblocked threads than are specified to
--     live in the pool. NB: this count includes the thread executing 'parallel_'.
--     This should minimize contention and hence pre-emption, while also preventing
--     starvation.
--
--  2. On return all actions have been performed.
--
--  3. The function returns in a timely manner as soon as all actions have
--     been performed.
--
--  4. The above properties are true even if 'parallel_' is used by an
--     action which is itself being executed by one of the parallel combinators.
--
-- If any of the IO actions throws an exception, the exception thrown by the first
-- failing action in the input list will be thrown by 'parallel_'.
parallel_ :: Pool -> [IO a] -> IO ()
parallel_ pool xs = parallel pool xs >> return ()

-- | As 'parallel_', but instead of throwing exceptions that are thrown by subcomputations,
-- they are returned in a data structure.
parallelE_ :: Pool -> [IO a] -> IO [Maybe SomeException]
parallelE_ pool = fmap (map (either Just (\_ -> Nothing))) . parallelE pool

-- | Run the list of computations in parallel, returning the results in the
-- same order as the corresponding actions.
--
-- Has the following properties:
--
--  1. Never creates more or less unblocked threads than are specified to
--     live in the pool. NB: this count includes the thread executing 'parallel'.
--     This should minimize contention and hence pre-emption, while also preventing
--     starvation.
--
--  2. On return all actions have been performed.
--
--  3. The function returns in a timely manner as soon as all actions have
--     been performed.
--
--  4. The above properties are true even if 'parallel' is used by an
--     action which is itself being executed by one of the parallel combinators.
--
-- If any of the IO actions throws an exception, the exception thrown by the first
-- failing action in the input list will be thrown by 'parallel'. This will not prevent
-- the other actions in the input list from being run (and hence we will wait on them to
-- complete before throwing).
parallel :: Pool -> [IO a] -> IO [a]
parallel pool xs = do
    ei_e_ress <- parallelE pool xs
    mapM (either throw return) ei_e_ress

-- | As 'parallel', but instead of throwing exceptions that are thrown by subcomputations,
-- they are returned in a data structure.
parallelE :: Pool -> [IO a] -> IO [Either SomeException a]
parallelE pool acts = mask $ \restore -> do
    resultvars <- forM acts $ \act -> do
        resultvar <- newEmptyMVar
        _tid <- forkIO $ bracket_ (killPoolWorkerFor pool) (spawnPoolWorkerFor pool) $ do
            ei_e_res <- try (restore act)
            -- Use tryPutMVar instead of putMVar so we get an exception if my brain has failed
            True <- tryPutMVar resultvar ei_e_res
            return ()
        return resultvar
    extraWorkerWhileBlocked pool (mapM takeMVar resultvars)

-- | Run the list of computations in parallel, returning the results in the
-- approximate order of completion.
--
-- Has the following properties:
--
--  1. Never creates more or less unblocked threads than are specified to
--     live in the pool. NB: this count includes the thread executing 'parallelInterleaved'.
--     This should minimize contention and hence pre-emption, while also preventing
--     starvation.
--
--  2. On return all actions have been performed.
--
--  3. The result of running actions appear in the list in undefined order, but which
--     is likely to be very similar to the order of completion.
--
--  3. The above properties are true even if 'parallelInterleaved' is used by an
--     action which is itself being executed by one of the parallel combinators.
--
-- If any of the IO actions throws an exception, the exception we first come across
-- will be thrown by 'parallelInterleaved'. This will not prevent the other actions in the
-- input list from being run (and hence we will wait on them to complete before throwing).
parallelInterleaved :: Pool -> [IO a] -> IO [a]
parallelInterleaved pool acts = do
    ei_e_ress <- parallelInterleavedE pool acts
    mapM (either throw return) ei_e_ress

-- | As 'parallelInterleaved', but instead of throwing exceptions that are thrown by subcomputations,
-- they are returned in a data structure.
parallelInterleavedE :: Pool -> [IO a] -> IO [Either SomeException a]
parallelInterleavedE pool acts = mask $ \restore -> do
    resultchan <- newChan
    forM_ acts $ \act -> do
        _tid <- forkIO $ bracket_ (killPoolWorkerFor pool) (spawnPoolWorkerFor pool) $ do
            ei_e_res <- try (restore act)
            writeChan resultchan ei_e_res
        return ()
    extraWorkerWhileBlocked pool (mapM (\_act -> readChan resultchan) acts)

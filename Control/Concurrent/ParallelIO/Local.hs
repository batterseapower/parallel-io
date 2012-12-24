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
    parallel_, parallelE_, parallel, parallelE,
    parallelInterleaved, parallelInterleavedE,
    parallelFirst, parallelFirstE,

    -- * Pool management
    Pool, withPool, startPool, stopPool,
    extraWorkerWhileBlocked,
    
    -- * Advanced pool management
    spawnPoolWorkerFor, killPoolWorkerFor
  ) where

import Control.Concurrent.ParallelIO.Compat

import Control.Concurrent
import Control.Exception
import qualified Control.Exception as E
import Control.Monad

import System.IO


catchNonThreadKilled :: IO a -> (SomeException -> IO a) -> IO a
catchNonThreadKilled act handler = act `E.catch` \e -> case fromException e of Just ThreadKilled -> throwIO e; _ -> handler e

onNonThreadKilledException :: IO a -> IO b -> IO a
onNonThreadKilledException act handler = catchNonThreadKilled act (\e -> handler >> throwIO e)

reflectExceptionsTo :: ThreadId -> IO () -> IO ()
reflectExceptionsTo tid act = catchNonThreadKilled act (throwTo tid)


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
--  5. If any of the IO actions throws an exception this does not prevent any of the
--     other actions from being performed.
--
--  6. If any of the IO actions throws an exception, the exception thrown by the first
--     failing action in the input list will be thrown by 'parallel_'. Importantly, at the
--     time the exception is thrown there is no guarantee that the other parallel actions
--     have completed.
--
--     The motivation for this choice is that waiting for the all threads to either return
--     or throw before throwing the first exception will almost always cause GHC to show the
--     "Blocked indefinitely in MVar operation" exception rather than the exception you care about.
--
--     The reason for this behaviour can be seen by considering this machine state:
--
--       1. The main thread has used the parallel combinators to spawn two threads, thread 1 and thread 2.
--          It is blocked on both of them waiting for them to return either a result or an exception via an MVar.
--
--       2. Thread 1 and thread 2 share another (empty) MVar, the "wait handle". Thread 2 is waiting on the handle,
--          while thread 2 will eventually put into the handle.
--     
--     Consider what happens when thread 1 is buggy and throws an exception before putting into the handle. Now
--     thread 2 is blocked indefinitely, and so the main thread is also blocked indefinetly waiting for the result
--     of thread 2. GHC has no choice but to throw the uninformative exception. However, what we really wanted to
--     see was the original exception thrown in thread 1!
--
--     By having the main thread abandon its wait for the results of the spawned threads as soon as the first exception
--     comes in, we give this exception a chance to actually be displayed.
parallel_ :: Pool -> [IO a] -> IO ()
parallel_ pool xs = parallel pool xs >> return ()

-- | As 'parallel_', but instead of throwing exceptions that are thrown by subcomputations,
-- they are returned in a data structure.
--
-- As a result, property 6 of 'parallel_' is not preserved, and therefore if your IO actions can depend on each other
-- and may throw exceptions your program may die with "blocked indefinitely" exceptions rather than informative messages.
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
--  5. If any of the IO actions throws an exception this does not prevent any of the
--     other actions from being performed.
--
--  6. If any of the IO actions throws an exception, the exception thrown by the first
--     failing action in the input list will be thrown by 'parallel'. Importantly, at the
--     time the exception is thrown there is no guarantee that the other parallel actions
--     have completed.
--
--     The motivation for this choice is that waiting for the all threads to either return
--     or throw before throwing the first exception will almost always cause GHC to show the
--     "Blocked indefinitely in MVar operation" exception rather than the exception you care about.
--
--     The reason for this behaviour can be seen by considering this machine state:
--
--       1. The main thread has used the parallel combinators to spawn two threads, thread 1 and thread 2.
--          It is blocked on both of them waiting for them to return either a result or an exception via an MVar.
--
--       2. Thread 1 and thread 2 share another (empty) MVar, the "wait handle". Thread 2 is waiting on the handle,
--          while thread 2 will eventually put into the handle.
--     
--     Consider what happens when thread 1 is buggy and throws an exception before putting into the handle. Now
--     thread 2 is blocked indefinitely, and so the main thread is also blocked indefinetly waiting for the result
--     of thread 2. GHC has no choice but to throw the uninformative exception. However, what we really wanted to
--     see was the original exception thrown in thread 1!
--
--     By having the main thread abandon its wait for the results of the spawned threads as soon as the first exception
--     comes in, we give this exception a chance to actually be displayed.
parallel :: Pool -> [IO a] -> IO [a]
parallel pool acts = mask $ \restore -> do
    main_tid <- myThreadId
    resultvars <- forM acts $ \act -> do
        resultvar <- newEmptyMVar
        _tid <- forkIO $ bracket_ (killPoolWorkerFor pool) (spawnPoolWorkerFor pool) $ reflectExceptionsTo main_tid $ do
            res <- restore act
            -- Use tryPutMVar instead of putMVar so we get an exception if my brain has failed
            True <- tryPutMVar resultvar res
            return ()
        return resultvar
    extraWorkerWhileBlocked pool (mapM takeMVar resultvars)

-- | As 'parallel', but instead of throwing exceptions that are thrown by subcomputations,
-- they are returned in a data structure.
--
-- As a result, property 6 of 'parallel' is not preserved, and therefore if your IO actions can depend on each other
-- and may throw exceptions your program may die with "blocked indefinitely" exceptions rather than informative messages.
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
--  4. The above properties are true even if 'parallelInterleaved' is used by an
--     action which is itself being executed by one of the parallel combinators.
--
--  5. If any of the IO actions throws an exception this does not prevent any of the
--     other actions from being performed.
--
--  6. If any of the IO actions throws an exception, the exception thrown by the first
--     failing action in the input list will be thrown by 'parallelInterleaved'. Importantly, at the
--     time the exception is thrown there is no guarantee that the other parallel actions
--     have completed.
--
--     The motivation for this choice is that waiting for the all threads to either return
--     or throw before throwing the first exception will almost always cause GHC to show the
--     "Blocked indefinitely in MVar operation" exception rather than the exception you care about.
--
--     The reason for this behaviour can be seen by considering this machine state:
--
--       1. The main thread has used the parallel combinators to spawn two threads, thread 1 and thread 2.
--          It is blocked on both of them waiting for them to return either a result or an exception via an MVar.
--
--       2. Thread 1 and thread 2 share another (empty) MVar, the "wait handle". Thread 2 is waiting on the handle,
--          while thread 1 will eventually put into the handle.
--     
--     Consider what happens when thread 1 is buggy and throws an exception before putting into the handle. Now
--     thread 2 is blocked indefinitely, and so the main thread is also blocked indefinetly waiting for the result
--     of thread 2. GHC has no choice but to throw the uninformative exception. However, what we really wanted to
--     see was the original exception thrown in thread 1!
--
--     By having the main thread abandon its wait for the results of the spawned threads as soon as the first exception
--     comes in, we give this exception a chance to actually be displayed.
parallelInterleaved :: Pool -> [IO a] -> IO [a]
parallelInterleaved pool acts = mask $ \restore -> do
    main_tid <- myThreadId
    resultchan <- newChan
    forM_ acts $ \act -> do
        _tid <- forkIO $ bracket_ (killPoolWorkerFor pool) (spawnPoolWorkerFor pool) $ reflectExceptionsTo main_tid $ do
            res <- restore act
            writeChan resultchan res
        return ()
    extraWorkerWhileBlocked pool (mapM (\_act -> readChan resultchan) acts)

-- | As 'parallelInterleaved', but instead of throwing exceptions that are thrown by subcomputations,
-- they are returned in a data structure.
--
-- As a result, property 6 of 'parallelInterleaved' is not preserved, and therefore if your IO actions can depend on each other
-- and may throw exceptions your program may die with "blocked indefinitely" exceptions rather than informative messages.
parallelInterleavedE :: Pool -> [IO a] -> IO [Either SomeException a]
parallelInterleavedE pool acts = mask $ \restore -> do
    resultchan <- newChan
    forM_ acts $ \act -> do
        _tid <- forkIO $ bracket_ (killPoolWorkerFor pool) (spawnPoolWorkerFor pool) $ do
            ei_e_res <- try (restore act)
            writeChan resultchan ei_e_res
        return ()
    extraWorkerWhileBlocked pool (mapM (\_act -> readChan resultchan) acts)

-- | Run the list of computations in parallel, returning the result of the first
-- thread that completes with (Just x), if any
--
-- Has the following properties:
--
--  1. Never creates more or less unblocked threads than are specified to
--     live in the pool. NB: this count includes the thread executing 'parallelInterleaved'.
--     This should minimize contention and hence pre-emption, while also preventing
--     starvation.
--
--  2. On return all actions have either been performed or cancelled (with ThreadKilled exceptions).
--
--  3. The above properties are true even if 'parallelFirst' is used by an
--     action which is itself being executed by one of the parallel combinators.
--
--  4. If any of the IO actions throws an exception, the exception thrown by the first
--     throwing action in the input list will be thrown by 'parallelFirst'. Importantly, at the
--     time the exception is thrown there is no guarantee that the other parallel actions
--     have been completed or cancelled.
--
--     The motivation for this choice is that waiting for the all threads to either return
--     or throw before throwing the first exception will almost always cause GHC to show the
--     "Blocked indefinitely in MVar operation" exception rather than the exception you care about.
--
--     The reason for this behaviour can be seen by considering this machine state:
--
--       1. The main thread has used the parallel combinators to spawn two threads, thread 1 and thread 2.
--          It is blocked on both of them waiting for them to return either a result or an exception via an MVar.
--
--       2. Thread 1 and thread 2 share another (empty) MVar, the "wait handle". Thread 2 is waiting on the handle,
--          while thread 1 will eventually put into the handle.
--     
--     Consider what happens when thread 1 is buggy and throws an exception before putting into the handle. Now
--     thread 2 is blocked indefinitely, and so the main thread is also blocked indefinetly waiting for the result
--     of thread 2. GHC has no choice but to throw the uninformative exception. However, what we really wanted to
--     see was the original exception thrown in thread 1!
--
--     By having the main thread abandon its wait for the results of the spawned threads as soon as the first exception
--     comes in, we give this exception a chance to actually be displayed.
parallelFirst :: Pool -> [IO (Maybe a)] -> IO (Maybe a)
parallelFirst pool acts = mask $ \restore -> do
    main_tid <- myThreadId
    resultvar <- newEmptyMVar
    (tids, waits) <- liftM unzip $ forM acts $ \act -> do
        wait_var <- newEmptyMVar
        tid <- forkIO $ flip onNonThreadKilledException (tryPutMVar resultvar Nothing) $                     -- If we throw an exception, unblock
                        bracket_ (killPoolWorkerFor pool) (spawnPoolWorkerFor pool >> putMVar wait_var ()) $ -- the main thread so it can rethrow it
                        reflectExceptionsTo main_tid $ do
            mb_res <- restore act
            case mb_res of
                Nothing  -> return ()
                Just res -> tryPutMVar resultvar (Just res) >> return ()
        return (tid, wait_var)
    forkIO $ mapM_ takeMVar waits >> tryPutMVar resultvar Nothing >> return ()
    mb_res <- extraWorkerWhileBlocked pool (takeMVar resultvar)
    mapM_ killThread tids
    return mb_res

-- | As 'parallelFirst', but instead of throwing exceptions that are thrown by subcomputations,
-- they are returned in a data structure.
--
-- As a result, property 4 of 'parallelFirst' is not preserved, and therefore if your IO actions can depend on each other
-- and may throw exceptions your program may die with "blocked indefinitely" exceptions rather than informative messages.
parallelFirstE :: Pool -> [IO (Maybe a)] -> IO (Maybe (Either SomeException a))
parallelFirstE pool acts = mask $ \restore -> do
    main_tid <- myThreadId
    resultvar <- newEmptyMVar
    (tids, waits) <- liftM unzip $ forM acts $ \act -> do
        wait_var <- newEmptyMVar
        tid <- forkIO $ bracket_ (killPoolWorkerFor pool) (spawnPoolWorkerFor pool >> putMVar wait_var ()) $ do
            ei_mb_res <- try (restore act)
            case ei_mb_res of
                -- NB: we aren't in danger of putting a "thread killed" exception into the MVar
                -- since we only kill the spawned threads *after* we have already taken from resultvar
                Left e           -> tryPutMVar resultvar (Just (Left e)) >> return ()
                Right Nothing    -> return ()
                Right (Just res) -> tryPutMVar resultvar (Just (Right res)) >> return ()
        return (tid, wait_var)
    forkIO $ mapM_ takeMVar waits >> tryPutMVar resultvar Nothing >> return ()
    mb_res <- extraWorkerWhileBlocked pool (takeMVar resultvar)
    mapM_ killThread tids
    return mb_res

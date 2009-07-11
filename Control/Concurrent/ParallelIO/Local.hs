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
-- The "Control.Concurrent.ParallelIO.Global" module is implemented
-- on top of this one by maintaining a shared global thread pool
-- with one thread per capability.
module Control.Concurrent.ParallelIO.Local (
    WorkItem, WorkQueue, Pool,
    withPool, startPool, stopPool,
    enqueueOnPool, spawnPoolWorkerFor,
    
    parallel_, parallel
  ) where

import Control.Concurrent
import Control.Exception.Extensible as E
import Control.Monad


-- | Type of work items you can put onto the queue. The 'Bool'
-- returned from the 'IO' action specifies whether the invoking
-- thread should terminate itself immediately.
type WorkItem = IO Bool

-- | A 'WorkQueue' is used to communicate 'WorkItem's to the workers.
type WorkQueue = Chan WorkItem

-- | The type of thread pools used by 'ParallelIO'.
-- The best way to construct one of these is using 'withPool'.
data Pool = Pool {
    pool_threadcount :: Int,
    pool_spawnedby :: ThreadId,
    pool_queue :: WorkQueue
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
startPool threadcount = do
    threadId <- myThreadId
    queue <- newChan
    let pool = Pool {
            pool_threadcount = threadcount,
            pool_spawnedby = threadId,
            pool_queue = queue
          }
    
    replicateM_ (threadcount - 1) (spawnPoolWorkerFor pool)
    return pool

-- | Clean up a thread pool. If you don't call this then no one holds the queue,
-- the queue gets GC'd, the threads find themselves blocked indefinitely, and you get
-- exceptions.
-- 
-- This cleanly shuts down the threads so the queue isn't important and you don't get
-- exceptions.
--
-- Only call this /after/ all users of the pool have completed, or your program may
-- block indefinitely.
stopPool :: Pool -> IO ()
stopPool pool = replicateM_ (pool_threadcount pool - 1) $ writeChan (pool_queue pool) $ return True

-- | A safe wrapper around 'startPool' and 'stopPool'. Executes an 'IO' action using a newly-created
-- pool with the specified number of threads and cleans it up at the end.
withPool :: Int -> (Pool -> IO a) -> IO a
withPool threadcount = E.bracket (startPool threadcount) stopPool


-- | Internal method for scheduling work on a pool.
enqueueOnPool :: Pool -> WorkItem -> IO ()
enqueueOnPool pool = writeChan (pool_queue pool)

-- | Internal method for adding extra unblocked threads to a pool if one is going to be
-- temporarily blocked.
spawnPoolWorkerFor :: Pool -> IO ()
spawnPoolWorkerFor pool = do
    forkIO $ workerLoop `E.catch` \(e :: E.SomeException) -> do
        putStrLn $ "Exception on thread: " ++ show e
        throwTo (pool_spawnedby pool) $ ErrorCall $ "Control.Concurrent.ParallelIO: parallel thread died.\n" ++ show e
    return ()
    where
        workerLoop :: IO ()
        workerLoop = do
            kill <- join $ readChan (pool_queue pool)
            unless kill workerLoop


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
--     action which is itself being executed by 'parallel_'.
parallel_ :: Pool -> [IO a] -> IO ()
parallel_ pool xs | pool_threadcount pool <= 1 = sequence_ xs
parallel_ _    [] = return ()
parallel_ _    [x] = x >> return ()
parallel_ pool (x1:xs) = do
    count <- newMVar $ length xs
    pause <- newEmptyMVar
    forM_ xs $ \x ->
        enqueueOnPool pool $ do
            x
            modifyMVar count $ \i -> do
                let i2 = i - 1
                    kill = i2 == 0
                when kill $ putMVar pause ()
                return (i2, kill)
    x1
    spawnPoolWorkerFor pool
    takeMVar pause

-- | Run the list of computations in parallel, returning the results in the
-- same order as the corresponding actions.
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
--  4. The above properties are true even if 'parallel' is used by an
--     action which is itself being executed by 'parallel'.
parallel :: Pool -> [IO a] -> IO [a]
parallel pool xs | pool_threadcount pool <= 1 = sequence xs
parallel _    [] = return []
parallel _    [x] = fmap return x
parallel pool (x1:xs) = do
    count <- newMVar $ length xs
    resultvars <- forM xs $ \x -> do
        resultvar <- newEmptyMVar
        enqueueOnPool pool $ do
            x >>= putMVar resultvar
            modifyMVar count $ \i -> let i' = i - 1 in return (i', i' == 0)
        return resultvar
    result1 <- x1
    spawnPoolWorkerFor pool
    fmap (result1:) $ mapM takeMVar resultvars

-- An alternative implementation of parallel_ might:
--
--  1. Avoid spawning an additional thread
--
--  2. Remove the need for the pause mvar
--
-- By having the thread invoking parallel_ also pull stuff from the
-- work pool, and poll the count variable after every item to see
-- if everything has been processed (which would cause it to stop
-- processing work pool items). However:
--
--  1. This is less timely, because the main thread might get stuck
--     processing a big work item not related to the current parallel_
--     invocation, and wouldn't poll (and return) until that was done.
--
--  2. It actually performs a bit less well too - or at least it did on
--     my benchmark with lots of cheap actions, where polling would
--     be relatively frequent. Went from 8.8s to 9.1s.
--
-- For posterity, the implementation was:
--
-- @
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
-- @
--
-- NB: in this scheme, kill is only True when the program is exiting.
{-# LANGUAGE CPP, ScopedTypeVariables #-}
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
    parallel_, parallel, parallelInterleaved,

    -- * Pool management
    Pool, withPool, startPool, stopPool,
    extraWorkerWhileBlocked,
    
    -- * Advanced pool management
    spawnPoolWorkerFor, killPoolWorkerFor
  ) where

import qualified Control.Concurrent.ParallelIO.ConcurrentSet as CS

import Control.Concurrent
import Control.Exception.Extensible as E
import Control.Monad

import System.IO


#if MIN_VERSION_base(4,3,0)
import Control.Exception ( mask )
#else
import Control.Exception ( blocked, block, unblock )

mask :: ((IO a -> IO a) -> IO b) -> IO b
mask io = blocked >>= \b -> if b then io id else block $ io unblock
#endif


-- | Type of work items that are put onto the queue internally. The 'Bool'
-- returned from the 'IO' action specifies whether the invoking
-- thread should terminate itself immediately.
type WorkItem = IO Bool

-- | A 'WorkQueue' is used to communicate 'WorkItem's to the workers.
type WorkQueue = CS.ConcurrentSet WorkItem

-- | A thread pool, containing a maximum number of threads. The best way to
-- construct one of these is using 'withPool'.
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
startPool threadcount
  | threadcount < 1 = error $ "startPool: thread count must be strictly positive (was " ++ show threadcount ++ ")"
  | otherwise = do
    threadId <- myThreadId
    queue <- CS.new
    let pool = Pool {
            pool_threadcount = threadcount,
            pool_spawnedby = threadId,
            pool_queue = queue
          }
    
    replicateM_ (threadcount - 1) (spawnPoolWorkerFor pool)
    return pool

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
withPool threadcount = E.bracket (startPool threadcount) stopPool


-- | Internal method for scheduling work on a pool.
enqueueOnPool :: Pool -> WorkItem -> IO ()
enqueueOnPool pool = CS.insert (pool_queue pool)

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
extraWorkerWhileBlocked pool wait = E.bracket (spawnPoolWorkerFor pool) (\() -> killPoolWorkerFor pool) (\() -> wait)

-- | Internal method for adding extra unblocked threads to a pool if one of the current
-- worker threads is going to be temporarily blocked. Unrestricted use of this is unsafe,
-- so we reccomend that you use the 'extraWorkerWhileBlocked' function instead if possible.
spawnPoolWorkerFor :: Pool -> IO ()
spawnPoolWorkerFor pool = do
    _ <- mask $ \restore -> forkIO $ restore workerLoop `E.catch` \(e :: E.SomeException) -> do
        hPutStrLn stderr $ "Exception on thread: " ++ show e
        throwTo (pool_spawnedby pool) $ ErrorCall $ "Control.Concurrent.ParallelIO: parallel thread died.\n" ++ show e
    return ()
    where
        workerLoop :: IO ()
        workerLoop = do
            kill <- join $ CS.delete (pool_queue pool)
            unless kill workerLoop

-- | Internal method for removing threads from a pool after one of the threads on the pool
-- becomes newly unblocked. Unrestricted use of this is unsafe, so we reccomend that you use
-- the 'extraWorkerWhileBlocked' function instead if possible.
killPoolWorkerFor :: Pool -> IO ()
killPoolWorkerFor pool = enqueueOnPool pool $ return True


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
parallel_ :: Pool -> [IO a] -> IO ()
parallel_ _    [] = return ()
-- It is very important that we *don't* include this special case!
-- The reason is that even if there is only one worker thread in the pool, one of
-- the items we process might depend on the ability to use extraWorkerWhileBlocked
-- to allow processing to continue even before it has finished executing.
--parallel_ pool xs | pool_threadcount pool <= 1 = sequence_ xs
parallel_ _    [x] = x >> return ()
parallel_ pool (x1:xs) = mask $ \restore -> do
    count <- newMVar $ length xs
    pause <- newEmptyMVar
    forM_ xs $ \x ->
        enqueueOnPool pool $ do
            _ <- restore x
            modifyMVar count $ \i -> do
                let i' = i - 1
                    kill = i' == 0
                when kill $ putMVar pause ()
                return (i', kill)
    _ <- restore x1
    -- NB: it is safe to spawn a worker because at least one will die - the
    -- length of xs must be strictly greater than 0.
    spawnPoolWorkerFor pool
    takeMVar pause

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
parallel :: Pool -> [IO a] -> IO [a]
parallel _    [] = return []
-- It is important that we do not include this special case (see parallel_ for why)
--parallel pool xs | pool_threadcount pool <= 1 = sequence xs
parallel _    [x] = fmap return x
parallel pool (x1:xs) = mask $ \restore -> do
    count <- newMVar $ length xs
    resultvars <- forM xs $ \x -> do
        resultvar <- newEmptyMVar
        enqueueOnPool pool $ do
            restore x >>= putMVar resultvar
            modifyMVar count $ \i -> let i' = i - 1 in return (i', i' == 0)
        return resultvar
    result1 <- restore x1
    -- NB: it is safe to spawn a worker because at least one will die - the
    -- length of xs must be strictly greater than 0.
    spawnPoolWorkerFor pool
    fmap (result1:) $ mapM takeMVar resultvars

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
parallelInterleaved :: Pool -> [IO a] -> IO [a]
parallelInterleaved _    [] = return []
-- It is important that we do not include this special case (see parallel_ for why)
--parallelInterleaved pool xs | pool_threadcount pool <= 1 = sequence xs
parallelInterleaved _    [x] = fmap return x
parallelInterleaved pool (x1:xs) = mask $ \restore -> do
    let thecount = length xs
    count <- newMVar $ thecount
    resultschan <- newChan
    forM_ xs $ \x -> do
        enqueueOnPool pool $ do
            restore x >>= writeChan resultschan
            modifyMVar count $ \i -> let i' = i - 1 in return (i', i' == 0)
    result1 <- restore x1
    -- NB: it is safe to spawn a worker because at least one will die - the
    -- length of xs must be strictly greater than 0.
    spawnPoolWorkerFor pool
    results <- fmap ((result1:) . take thecount) $ getChanContents resultschan
    return $ seqList results

seqList :: [a] -> [a]
seqList []     = []
seqList (x:xs) = x `seq` xs' `seq` (x:xs')
  where xs' = seqList xs

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
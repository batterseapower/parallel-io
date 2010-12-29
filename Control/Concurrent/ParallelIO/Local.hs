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
import qualified Control.Concurrent.ParallelIO.ConcurrentCollection as CC

import Control.Concurrent
import Control.Exception
import Control.Monad

import Data.IORef

import System.IO

import Prelude hiding (catch)


-- TODO: I should deal nicely with exceptions raised by the actions on other threads.
-- Probably I should provide variants of the functions that report exceptions in lieu
-- of values.
--
-- When I introduce this, I want to preserve the current behaviour that causes the
-- application to die promptly if we are using the unsafe variants of the combinators,
-- and one of the nested actions dies.


-- | Type of work items that are put onto the queue internally. The 'Bool'
-- returned from the 'IO' action specifies whether the invoking
-- thread should terminate itself immediately.
--
-- INVARIANT: all 'WorkItem's do not throw synchronous exceptions. It is acceptable
-- for them to throw asynchronous exceptions and to be interruptible.
type WorkItem = IO Bool

-- | A 'WorkQueue' is used to communicate 'WorkItem's to the workers.
--type WorkQueue = CC.Chan WorkItem
type WorkQueue = CC.ConcurrentSet WorkItem

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
    queue <- CC.new
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
withPool threadcount = bracket (startPool threadcount) stopPool


-- | Internal method for scheduling work on a pool.
enqueueOnPool :: Pool -> WorkItem -> IO ()
enqueueOnPool pool = CC.insert (pool_queue pool)

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
extraWorkerWhileBlocked pool wait = bracket (spawnPoolWorkerFor pool) (\() -> killPoolWorkerFor pool) (\() -> wait)

-- | Internal method for adding extra unblocked threads to a pool if one of the current
-- worker threads is going to be temporarily blocked. Unrestricted use of this is unsafe,
-- so we recommend that you use the 'extraWorkerWhileBlocked' function instead if possible.
spawnPoolWorkerFor :: Pool -> IO ()
spawnPoolWorkerFor pool = {- putStrLn "spawnPoolWorkerFor" >> -} do
    _ <- mask $ \restore -> forkIO $ restore workerLoop `catch` \(e :: SomeException) -> do
        tid <- myThreadId
        hPutStrLn stderr $ "Exception on " ++ show tid ++ ": " ++ show e
        throwTo (pool_spawnedby pool) $ ErrorCall $ "Control.Concurrent.ParallelIO: parallel thread died.\n" ++ show e
    return ()
    where
        workerLoop :: IO ()
        workerLoop = do
            --tid <- myThreadId
            --hPutStrLn stderr $ "[waiting] " ++ show tid
            work_item <- CC.delete (pool_queue pool)
            --hPutStrLn stderr $ "[working] " ++ show tid
            
            -- If we get an asynchronous exception on a worker thread, don't make any attempt to handle it: just die.
            -- The one concession we make is that we are careful not to lose work items from the queue.
            kill <- work_item `onException` CC.insert (pool_queue pool) work_item
            unless kill workerLoop

-- | Internal method for removing threads from a pool after one of the threads on the pool
-- becomes newly unblocked. Unrestricted use of this is unsafe, so we reccomend that you use
-- the 'extraWorkerWhileBlocked' function instead if possible.
killPoolWorkerFor :: Pool -> IO ()
killPoolWorkerFor pool = {- putStrLn "killPoolWorkerFor" >> -} enqueueOnPool pool (return True)


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
-- failing action in the input list will be thrown by 'parallel'.
parallel :: Pool -> [IO a] -> IO [a]
parallel pool xs = do
    ei_e_ress <- parallelE pool xs
    mapM (either throw return) ei_e_ress

-- | As 'parallel', but instead of throwing exceptions that are thrown by subcomputations,
-- they are returned in a data structure.
parallelE :: Pool -> [IO a] -> IO [Either SomeException a]
parallelE _    [] = return []
-- It is very important that we *don't* include this special case!
-- The reason is that even if there is only one worker thread in the pool, one of
-- the items we process might depend on the ability to use extraWorkerWhileBlocked
-- to allow processing to continue even before it has finished executing.
--parallelE pool xs | pool_threadcount pool <= 1 = sequence xs
parallelE _    [x] = fmap return (try x)
parallelE pool (x1:xs) = mask $ \restore -> do
    count <- newIORef $ length xs
    resultvars <- forM xs $ \x -> do
        resultvar <- newEmptyMVar
        enqueueOnPool pool $ do
            ei_e_res <- try (restore x)
            -- Use tryPutMVar instead of putMVar so we get an exception if my brain has failed
            -- This also has the bonus that tryPutMVar is non-blocking, so we cannot get any
            -- asynchronous exceptions from it (it is not "interruptable")
            True <- tryPutMVar resultvar ei_e_res
            atomicModifyIORef count $ \i -> let i' = i - 1 in (i', i' == 0)
        return resultvar
    ei_e_res1 <- try (restore x1)
    -- NB: it is safe to spawn a worker because at least one will die - the
    -- length of xs must be strictly greater than 0.
    spawnPoolWorkerFor pool
    fmap (ei_e_res1:) $ mapM takeMVar resultvars

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
-- If any of the IO actions throws an exception, the exception thrown by the first
-- completing action in the input list will be thrown by 'parallelInterleaved'.
parallelInterleaved :: Pool -> [IO a] -> IO [a]
parallelInterleaved pool xs = do
    ei_e_ress <- parallelInterleavedE_lazy pool xs
    mapM (either throw return) ei_e_ress


-- | As 'parallelInterleaved', but instead of throwing exceptions that are thrown by subcomputations,
-- they are returned in a data structure.
parallelInterleavedE, parallelInterleavedE_lazy :: Pool -> [IO a] -> IO [Either SomeException a]
parallelInterleavedE pool xs = do
    ei_e_ress <- parallelInterleavedE_lazy pool xs
    mapM return ei_e_ress -- Force the output list: we should not return until all actions are done

parallelInterleavedE_lazy _    [] = return []
-- It is important that we do not include this special case (see parallel for why)
--parallelInterleaved pool xs | pool_threadcount pool <= 1 = sequence xs
parallelInterleavedE_lazy _    [x] = fmap return (try x)
parallelInterleavedE_lazy pool (x1:xs) = mask $ \restore -> do
    let thecount = length xs
    count <- newIORef (length xs)
    resultschan <- newChan
    forM_ xs $ \x -> do
        enqueueOnPool pool $ do
            ei_e_res <- try (restore x)
            -- Although writeChan is interruptible, it unblocks promptly
            writeChan resultschan ei_e_res
            atomicModifyIORef count $ \i -> let i' = i - 1 in (i', i' == 0)
    ei_e_res1 <- try (restore x1)
    -- NB: it is safe to spawn a worker because at least one will die - the
    -- length of xs must be strictly greater than 0.
    spawnPoolWorkerFor pool
    -- Yield results as they are output to the channel
    ei_e_ress_infinite <- getChanContents resultschan
    return (ei_e_res1:take thecount ei_e_ress_infinite)

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

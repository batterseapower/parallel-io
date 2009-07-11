module Control.Concurrent.ParallelIO.Safe (
    Pool,
    withPool, startPool, stopPool,
    enqueueOnPool,
    spawnPoolWorkerFor
  ) where

import Control.Concurrent
import Control.Exception.Extensible as E
import Control.Monad


type WorkItem = IO Bool
type WorkQueue = Chan WorkItem

data Pool = Pool {
    pool_threadcount :: Int,
    pool_spawnedby :: ThreadId,
    pool_queue :: WorkQueue
  }

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

-- If you don't call this then no one holds the queue, the queue gets
-- GC'd, the threads find themselves blocked indefinately, and you get
-- exceptions. This cleanly shuts down the threads, then the queue isn't important.
-- Only call this AFTER all parallel_ calls have completed.
stopPool :: Pool -> IO ()
stopPool pool = replicateM_ (pool_threadcount pool - 1) $ writeChan (pool_queue pool) $ return True

withPool :: Int -> (Pool -> IO a) -> IO a
withPool threadcount = E.bracket (startPool threadcount) stopPool

enqueueOnPool :: Pool -> WorkItem -> IO ()
enqueueOnPool pool = writeChan (pool_queue pool)

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
    
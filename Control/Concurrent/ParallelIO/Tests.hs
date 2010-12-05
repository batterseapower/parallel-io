{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Main where

import Data.IORef
import Data.List

import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit ((@?))

import GHC.Conc

import Control.Monad

import Control.Concurrent.MVar
import qualified Control.Concurrent.ParallelIO.Global as Global
import Control.Concurrent.ParallelIO.Local


main :: IO ()
main = do
    defaultMain tests
    Global.stopGlobalPool

tests :: [Test]
tests = [
          testCase "parallel_ executes correct number of actions"       $ repeatTest parallel__execution_count_correct
        , testCase "parallel_ doesn't spawn too many threads"           $ repeatTest parallel__doesnt_spawn_too_many_threads
        , testCase "parallel executes correct actions"                  $ repeatTest parallel_executes_correct_actions
        , testCase "parallel doesn't spawn too many threads"            $ repeatTest parallel_doesnt_spawn_too_many_threads
        , testCase "parallelInterleaved executes correct actions"       $ repeatTest parallelInterleaved_executes_correct_actions
        , testCase "parallelInterleaved doesn't spawn too many threads" $ repeatTest parallelInterleaved_doesnt_spawn_too_many_threads
        , testCase "parallel with one worker can be blocked"            $ parallel_with_one_worker_can_be_blocked
        , testCase "parallel_ with one worker can be blocked"           $ parallel__with_one_worker_can_be_blocked
        ]

parallel__execution_count_correct n = do
    ref <- newIORef 0
    Global.parallel_ (replicate n (atomicModifyIORef_ ref (+ 1)))
    fmap (==n) $ readIORef ref

parallel_executes_correct_actions n = fmap (expected ==) actual
  where actual = Global.parallel (map (return . (+1)) [0..n])
        expected = [(1 :: Int)..n + 1]

parallelInterleaved_executes_correct_actions n = fmap ((expected ==) . sort) actual
  where actual = Global.parallelInterleaved (map (return . (+1)) [0..n])
        expected = [(1 :: Int)..n + 1]

parallel__doesnt_spawn_too_many_threads = doesnt_spawn_too_many_threads parallel_
parallel_doesnt_spawn_too_many_threads = doesnt_spawn_too_many_threads parallel
parallelInterleaved_doesnt_spawn_too_many_threads = doesnt_spawn_too_many_threads parallelInterleaved

doesnt_spawn_too_many_threads the_parallel n = do
    threadcountref <- newIORef 0
    maxref <- newIORef 0
    -- NB: we use a local pool rather than the global one because otherwise we get interference effects
    -- when we run the testsuite in parallel
    withPool numCapabilities $ \pool -> do
        _ <- the_parallel pool $ replicate n $ do
            tc' <- atomicModifyIORef_ threadcountref (+ 1)
            _ <- atomicModifyIORef_ maxref (`max` tc')
            -- This delay and 'yield' combination was experimentally determined. The test
            -- can and does still nondeterministically fail with a non-zero probability
            -- dependening on runtime scheduling behaviour. It seems that the first instance
            -- of this test to run in the process is especially vulnerable.
            yield
            threadDelay 20000
            yield
            atomicModifyIORef_ threadcountref (\tc -> tc - 1)
        seenmax <- readIORef maxref
        let expected_max_concurrent_threads = numCapabilities `min` n
        if expected_max_concurrent_threads == seenmax
         then return True
         else putStrLn ("Expected at most " ++ show expected_max_concurrent_threads ++ ", got " ++ show seenmax) >> return False

parallel_with_one_worker_can_be_blocked = with_one_worker_can_be_blocked parallel
parallel__with_one_worker_can_be_blocked = with_one_worker_can_be_blocked parallel_

-- This test is based on a specific bug I observed in the library. The problem was that I was special casing
-- pools with thread counts <= 1 to just use sequence/sequence_, but that doesn't give the right semantics if
-- the user is able to call extraWorkerWhileBlocked!
with_one_worker_can_be_blocked the_parallel = withPool 1 $ \pool -> do
    wait <- newEmptyMVar
    the_parallel pool [extraWorkerWhileBlocked pool (takeMVar wait), putMVar wait ()]
    return ()

atomicModifyIORef_ :: IORef a -> (a -> a) -> IO a
atomicModifyIORef_ ref f = atomicModifyIORef ref (\x -> let x' = f x in x' `seq` (x', x'))

repeatTest :: (Int -> IO Bool) -> IO ()
repeatTest testcase = forM_ [0..100] $ \n -> testcase n @? "n=" ++ show n

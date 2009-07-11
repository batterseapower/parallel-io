module Main where

import Data.IORef
import Data.List

import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit ((@?))

import GHC.Conc

import Control.Monad

import Control.Concurrent.ParallelIO.Global


main :: IO ()
main = do
    defaultMain tests
    stopGlobalPool

tests :: [Test]
tests = [ testCase "parallel_ executes correct number of actions"       $ repeatTest parallel__execution_count_correct
        , testCase "parallel_ doesn't spawn too many threads"           $ repeatTest parallel__doesnt_spawn_too_many_threads
        , testCase "parallel executes correct actions"                  $ repeatTest parallel_executes_correct_actions
        , testCase "parallel doesn't spawn too many threads"            $ repeatTest parallel_doesnt_spawn_too_many_threads
        --, testCase "parallelInterleaved executes correct actions"       $ repeatTest parallelInterleaved_executes_correct_actions
        --, testCase "parallelInterleaved doesn't spawn too many threads" $ repeatTest parallelInterleaved_doesnt_spawn_too_many_threads
        ]

parallel__execution_count_correct n = do
    ref <- newIORef 0
    parallel_ (replicate n (atomicModifyIORef_ ref (+ 1)))
    fmap (==n) $ readIORef ref

parallel_executes_correct_actions n = fmap (expected ==) actual
  where actual = parallel (map (return . (+1)) [0..n])
        expected = [(1 :: Int)..n + 1]

parallelInterleaved_executes_correct_actions n = fmap ((expected ==) . sort) actual
  where actual = parallelInterleaved (map (return . (+1)) [0..n])
        expected = [(1 :: Int)..n + 1]

parallel__doesnt_spawn_too_many_threads = doesnt_spawn_too_many_threads parallel_
parallel_doesnt_spawn_too_many_threads = doesnt_spawn_too_many_threads parallel
parallelInterleaved_doesnt_spawn_too_many_threads = doesnt_spawn_too_many_threads parallelInterleaved

doesnt_spawn_too_many_threads the_parallel n = do
    threadcountref <- newIORef 0
    maxref <- newIORef 0
    the_parallel $ replicate n $ do
        tc' <- atomicModifyIORef_ threadcountref (+ 1)
        atomicModifyIORef_ maxref (`max` tc')
        -- This delay and 'yield' combination was experimentally determined. The test
        -- can and does still nondeterministically fail with a non-zero probability
        -- dependening on runtime scheduling behaviour. It seems that the first instance
        -- of this test to run in the process is especially vulnerable.
        yield
        threadDelay 20000
        yield
        atomicModifyIORef_ threadcountref (\tc -> tc - 1)
    seenmax <- readIORef maxref
    return $ (numCapabilities `min` n) == seenmax


atomicModifyIORef_ :: IORef a -> (a -> a) -> IO a
atomicModifyIORef_ ref f = atomicModifyIORef ref (\x -> let x' = f x in x' `seq` (x', x'))

repeatTest :: (Int -> IO Bool) -> IO ()
repeatTest testcase = forM_ [0..100] $ \n -> testcase n @? show n

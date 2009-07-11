module Main where

import Data.IORef

import Test.Framework
import Test.Framework.Providers.QuickCheck
import Test.QuickCheck

import System.IO.Unsafe

import GHC.Conc

import Control.Concurrent.ParallelIO.Global


main :: IO ()
main = do
    defaultMain tests
    stopGlobalPool

atomicModifyIORef_ :: IORef a -> (a -> a) -> IO a
atomicModifyIORef_ ref f = atomicModifyIORef ref (\x -> let x' = f x in (x', x'))

tests :: [Test]
tests = [ testProperty "parallel_ executes correct number of actions" $ \n -> n >= 0 ==> parallel__execution_count_correct n
        , testProperty "parallel_ doesn't spawn too many threads"     $ \n -> n >= 0 ==> parallel__doesnt_spawn_too_many_threads n
        , testProperty "parallel executes correct actions"            $ \n -> n >= 0 ==> parallel_executes_correct_actions n
        , testProperty "parallel doesn't spawn too many threads"      $ \n -> n >= 0 ==> parallel_doesnt_spawn_too_many_threads n
        ]

parallel__execution_count_correct n = unsafePerformIO $ do
    ref <- newIORef 0
    parallel_ (replicate n (atomicModifyIORef_ ref (+ 1)))
    fmap (==n) $ readIORef ref

parallel_executes_correct_actions n = 
    unsafePerformIO (parallel (map (return . (+1)) [0..n])) == [(1 :: Int)..n + 1]

parallel__doesnt_spawn_too_many_threads = doesnt_spawn_too_many_threads parallel_
parallel_doesnt_spawn_too_many_threads = doesnt_spawn_too_many_threads parallel

doesnt_spawn_too_many_threads the_parallel n = unsafePerformIO $ do
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
    seenmax `seq` return $ (numCapabilities `min` n) == seenmax

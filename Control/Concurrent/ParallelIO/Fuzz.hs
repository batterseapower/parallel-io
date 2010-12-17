module Main where

import Data.IORef
import qualified Numeric

import System.Random

import Control.Concurrent
import Control.Concurrent.ParallelIO.Local

import Control.Monad


-- | Range for number of threads to spawn
sPAWN_RANGE = (0, 100)

-- | Delay range in microseconds
dELAY_RANGE = (0, 1000000)

-- | Out of 100 parallel actions, how many should recursively spawn?
sPAWN_PERCENTAGE :: Int
sPAWN_PERCENTAGE = 2

-- | Number of threads to have processing work items
mAX_WORKERS = 3


showFloat :: RealFloat a => a -> String
showFloat x = Numeric.showFFloat (Just 2) x ""


expected :: Fractional b => (Int, Int) -> b
expected (top, bottom) = fromIntegral (top + bottom) / 2

main :: IO ()
main = do
    -- Birth rate is the rate at which new work items enter the queue
    putStrLn $ "Expected birth rate: " ++ showFloat ((expected sPAWN_RANGE * (fromIntegral sPAWN_PERCENTAGE / 100) * fromIntegral mAX_WORKERS) / (expected dELAY_RANGE / 1000000) :: Double) ++ " items/second"
    -- Service rate is the rate at which work items are removed from the pool
    putStrLn $ "Expected service rate: " ++ showFloat (fromIntegral mAX_WORKERS / (expected dELAY_RANGE / 1000000) :: Double) ++ " items/second"
    -- We are balanced on average if birth rate == service rate, i.e. expected sPAWN_RANGE * (fromIntegral sPAWN_PERCENTAGE / 100) == 1
    putStrLn $ "Balance factor (should be 1): " ++ showFloat (expected sPAWN_RANGE * (fromIntegral sPAWN_PERCENTAGE / 100) :: Double)
    withPool mAX_WORKERS $ \pool -> forever (fuzz pool)

fuzz pool = do
    n <- randomRIO sPAWN_RANGE
    tid <- myThreadId
    putStrLn $ show tid ++ ":\t" ++ show n
    parallel_ pool $ flip map [1..n] $ \i -> do
        should_spawn <- fmap (<= sPAWN_PERCENTAGE) $ randomRIO (1, 100)
        nested_tid <- myThreadId
        
        putStrLn $ show nested_tid ++ ":\trunning " ++ show i ++ if should_spawn then " (recursing)" else ""
        
        randomRIO dELAY_RANGE >>= threadDelay
        
        putStrLn $ show nested_tid ++ ":\twoke up"
        
        when should_spawn $ fuzz pool

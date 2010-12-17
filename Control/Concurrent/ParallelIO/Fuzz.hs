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


showFloat2 :: RealFloat a => a -> String
showFloat2 x = Numeric.showFFloat (Just 2) x ""


expected :: Fractional b => (Int, Int) -> b
expected (top, bottom) = fromIntegral (top + bottom) / 2

main :: IO ()
main = do
    -- Birth rate is the rate at which new work items enter the queue
    putStrLn $ "Expected birth rate: " ++ showFloat2 ((expected sPAWN_RANGE * (fromIntegral sPAWN_PERCENTAGE / 100) * fromIntegral mAX_WORKERS) / expected dELAY_RANGE :: Double)
    -- Service rate is the rate at which work items are removed from the pool
    putStrLn $ "Expected service rate: " ++ showFloat2 (fromIntegral mAX_WORKERS / expected dELAY_RANGE :: Double)
    -- We are balanced on average if birth rate == service rate, i.e. expected sPAWN_RANGE * (fromIntegral sPAWN_PERCENTAGE / 100) == 1
    putStrLn $ "Balance factor (should be 1): " ++ showFloat2 (expected sPAWN_RANGE * (fromIntegral sPAWN_PERCENTAGE / 100) :: Double)
    withPool mAX_WORKERS $ \pool -> forever (fuzz pool)

fuzz pool = do
    n <- randomRIO sPAWN_RANGE
    parallel_ pool $ replicate n $ do
        should_spawn <- fmap (<= sPAWN_PERCENTAGE) $ randomRIO (1, 100)
        randomRIO dELAY_RANGE >>= threadDelay
        when should_spawn $ fuzz pool

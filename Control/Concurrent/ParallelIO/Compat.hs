{-# LANGUAGE CPP #-}
module Control.Concurrent.ParallelIO.Compat (
    mask, mask_
  ) where

#if MIN_VERSION_base(4,3,0)
import Control.Exception ( mask, mask_ )
#else
import Control.Exception ( blocked, block, unblock )

mask :: ((IO a -> IO a) -> IO b) -> IO b
mask io = blocked >>= \b -> if b then io id else block $ io unblock

mask_ :: IO a -> IO a
mask_ io = mask $ \_ -> io
#endif

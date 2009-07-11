module Control.Concurrent.ParallelIO (
    module Control.Concurrent.ParallelIO.Global
  ) where

-- By default, just export the user-friendly Global interface.
-- Those who want more power can import Local explicitly.
import Control.Concurrent.ParallelIO.Global
      
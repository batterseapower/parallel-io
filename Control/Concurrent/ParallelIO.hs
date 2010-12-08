-- | Combinators for executing IO actions in parallel on a thread pool.
--
-- This module just reexports "Control.Concurrent.ParallelIO.Global": this contains versions of
-- the combinators that make use of a single global thread pool with as many threads as there are
-- capabilities.
--
-- For finer-grained control, you can use "Control.Concurrent.ParallelIO.Local" instead, which
-- gives you control over the creation of the pool.
module Control.Concurrent.ParallelIO (
    module Control.Concurrent.ParallelIO.Global
  ) where

-- By default, just export the user-friendly Global interface.
-- Those who want more power can import Local explicitly.
import Control.Concurrent.ParallelIO.Global
      
module Control.Concurrent.ParallelIO.ConcurrentCollection (
    ConcurrentSet, Chan, ConcurrentCollection(..)
  ) where

import Control.Concurrent.ParallelIO.Compat

import Control.Concurrent.MVar
import Control.Concurrent.Chan
import Control.Monad

import qualified Data.IntMap as IM

import System.Random


class ConcurrentCollection p where
    new :: IO (p a)
    insert :: p a -> a -> IO ()
    delete :: p a -> IO a


-- | A set that elements can be added to and remove from concurrently.
--
-- The main difference between this and a queue is that 'ConcurrentSet' does not
-- make any guarantees about the order in which things will come out -- in fact,
-- it will go out of its way to make sure that they are unordered!
--
-- The reason that I use this primitive rather than 'Chan' is that:
--   1) At Standard Chartered we saw intermitted deadlocks when using 'Chan',
--      but Neil tells me that he stopped seeing them when they moved to a 'ConcurrentSet'
--      like thing. We never found the reason for the deadlocks though...
--   2) It's better to dequeue parallel tasks in pseudo random order for many
--      common applications, because (e.g. in Shake) lots of tasks that require the same
--      machine resources (i.e. CPU or RAM) tend to be next to each other in the list.
--      Thus, reducing access locality means that we tend to choose tasks that require
--      different resources.
data ConcurrentSet a = CS (MVar (StdGen, Contents (IM.IntMap a)))

data Contents a = EmptyWithWaiters (MVar ())
                | NonEmpty a

instance ConcurrentCollection ConcurrentSet where
    new = fmap CS $ liftM2 (\gen mvar -> (gen, EmptyWithWaiters mvar)) newStdGen newEmptyMVar >>= newMVar

    -- We don't mask asynchronous exceptions here because it's OK if we signal the wait_mvar
    -- but the set still doesn't contain anything: the readers (i.e. in "delete") will just
    -- discover that and start waiting again, just as if another thread had deleted before
    -- they got a chance to read from a newly non-empty set
    insert (CS set_mvar) x = modifyMVar_ set_mvar go
      where go (gen, contents) = do
                let (i, gen') = random gen
                case contents of
                  EmptyWithWaiters wait_mvar -> do
                    -- Wake up all waiters (if any): any one of them may want this item
                    --
                    -- NB: we don't use putMvar here (even though it would be safe) because
                    -- this way I get an obvious exception if I've done something daft.
                    True <- tryPutMVar wait_mvar ()
                    return (gen', NonEmpty (IM.singleton i x))
                  NonEmpty ys -> return (gen', NonEmpty (IM.insert i x ys))

    delete (CS set_mvar) = loop
      where
        loop = do
            contents <- modifyMVar set_mvar peek_inside
            case contents of
                EmptyWithWaiters wait_mvar -> do
                    -- NB: it's very important that we don't do this while we are holding the set_mvar!
                    --
                    -- We are careful to readMVar here rather than takeMVar, because *there may be more
                    -- than one waiter*. This does lead to a bit of a scrummage, because every single
                    -- waiter will get woken up and go for newly-added data simultaneously, but the alternative
                    -- is disconcertingly subtle.
                    () <- readMVar wait_mvar
                    
                    -- Someone put data in the MVar, but we might have to wait again if someone snaffles
                    -- it before we got there.
                    --
                    -- TODO: make this fairer -- there is definite starvation potential here, though it
                    -- doesn't matter for the application I have in mind (Shake)
                    loop
                NonEmpty x -> return x
        
        peek_inside (gen, EmptyWithWaiters wait_mvar) = return ((gen, EmptyWithWaiters wait_mvar), EmptyWithWaiters wait_mvar)
        peek_inside (gen, NonEmpty xs) = do
            let (chosen, xs') = IM.deleteFindMin xs
            new_value <- if IM.null xs'
                          then fmap EmptyWithWaiters newEmptyMVar
                          else return (NonEmpty xs')
            return ((gen, new_value), NonEmpty chosen)


instance ConcurrentCollection Chan where
    new = newChan
    insert = writeChan
    delete = readChan

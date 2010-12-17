module Control.Concurrent.ParallelIO.ConcurrentCollection (
    ConcurrentSet, Chan, ConcurrentCollection(..)
  ) where

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
data ConcurrentSet a = CS (MVar (StdGen, Either (MVar ()) (IM.IntMap a)))

instance ConcurrentCollection ConcurrentSet where
    new = fmap CS $ liftM2 (\gen mvar -> (gen, Left mvar)) newStdGen newEmptyMVar >>= newMVar

    insert (CS set_mvar) x = modifyMVar_ set_mvar go
      where go (gen, ei_mvar_ys) = do
                let (i, gen') = random gen
                case ei_mvar_ys of
                  Left wait_mvar -> do
                    -- Wake up all waiters (if any): any one of them may want this item
                    putMVar wait_mvar ()
                    return (gen', Right (IM.singleton i x))
                  Right ys -> return (gen', Right (IM.insert i x ys))

    delete (CS set_mvar) = loop
      where
        loop = do
            ei_wait_x <- modifyMVar set_mvar go
            case ei_wait_x of
                Left wait_mvar -> do
                    -- NB: it's very important that we don't do this while we are holding the set_mvar!
                    takeMVar wait_mvar
                    -- Someone put data in the MVar, but we might have to wait again if someone snaffles
                    -- it before we got there.
                    --
                    -- TODO: make this fairer -- there is definite starvation potential here, though it
                    -- doesn't matter for the application I have in mind (Shake)
                    loop
                Right x -> return x
        
        go (gen, Left wait_mvar) = return ((gen, Left wait_mvar), Left wait_mvar)
        go (gen, Right xs) = do
            let (chosen, xs') = IM.deleteFindMin xs
            new_value <- if IM.null xs'
                          then fmap Left newEmptyMVar
                          else return (Right xs')
            return ((gen, new_value), Right chosen)


instance ConcurrentCollection Chan where
    new = newChan
    insert = writeChan
    delete = readChan

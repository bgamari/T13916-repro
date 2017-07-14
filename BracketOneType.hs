{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{- |
Module      : BracketOneType
Description : Handling multiple environments with bracket-like apis
Maintainer  : robertkennedy@clearwateranalytics.com
Stability   : stable

This module is meant for ie Sql or mongo connections, where you may wish for some number of easy to grab
environments. In particular, this assumes your connection has some initialization/release functions

Shortened for bug submission by removing the "Lax" data type and associated cache type

This module does not create bugs
-}
module BracketOneType (
    -- * Data Types
    Spawner(..), Limit(..), Cache,
    -- * Usage
    withEnvCache, withEnv
    ) where

import Control.Concurrent.STM
import Control.Concurrent.STM.TSem
import Control.Exception hiding (handle)
import Data.Vector (Vector)
import qualified Data.Vector as Vector

-- * Data Types
data Limit = Hard {getLimit :: {-# unpack #-} !Int}

data Spawner env = Spawner
    { maker  :: IO env
    , killer :: env -> IO ()
    , isDead :: env -> IO Bool
    }

type VCache env = Vector (TMVar env)
data Cache env = Limited   { spawner :: Spawner env
                           , vcache :: {-# unpack #-} !(VCache env)
                           , envsem :: TSem
                           }

-- ** Initialization
withEnvCache :: Limit -> Spawner env -> (Cache env -> IO a) -> IO a
withEnvCache limit spawner = bracket starter releaseCache
    where starter = case limit of
            Hard n -> Limited spawner <$> initializeEmptyCache n <*> atomically (newTSem n)

-- ** Using a single value
withEnv :: Cache env -> (env -> IO a) -> IO a
withEnv cache = case cache of
    Limited{..}   -> withEnvLimited   spawner vcache envsem

-- *** Limited
-- | Takes an env and returns it on completion of the function.
-- If all envs are already taken, this will wait. This should have a constant number of environments
--
-- @since 0.3.6
withEnvLimited :: Spawner env -> VCache env -> TSem -> (env -> IO a) -> IO a
withEnvLimited spawner vcache envsem = bracket taker putter
  where
    taker = limitMakeEnv spawner vcache envsem
    putter env = atomically $ putEnv vcache env

limitMakeEnv :: Spawner env -> VCache env -> TSem -> IO env
limitMakeEnv Spawner{..} vcache envsem = go
  where
    go = do
        eenvpermission <- atomically $ ( Left  <$> takeEnv  vcache )
                              `orElse` ( Right <$> waitTSem envsem )
        case eenvpermission of
            Right () -> maker
            Left env -> do
                -- Given our env, we check if it's dead. If it's not, we are done and return it.
                -- If it is dead, we release it, signal that a new env can be created, and then recurse
                isdead <- isDead env
                if not isdead then return env
                    else do
                         killer env
                         atomically $ signalTSem envsem
                         go

-- * Low level
initializeEmptyCache :: Int -> IO (VCache env)
initializeEmptyCache n | n < 1     = return mempty
                       | otherwise = Vector.replicateM n newEmptyTMVarIO

takeEnv :: VCache env -> STM env
takeEnv = Vector.foldl folding retry
    where folding m stmenv = m `orElse` takeTMVar stmenv

putEnv :: VCache env -> env -> STM ()
putEnv cache env = Vector.foldl folding retry cache
    where folding m stmenv = m `orElse` putTMVar stmenv env


releaseCache :: Cache env -> IO ()
releaseCache cache = Vector.mapM_ qkRelease (vcache cache)
    where qkRelease tenv = atomically (tryTakeTMVar tenv)
                       >>= maybe (return ()) (killer $ spawner cache)

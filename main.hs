module Main where

import Data.IORef
import Bracket
import System.IO.Unsafe
import Control.Concurrent.STM
import Control.Concurrent.Async
import Control.Concurrent
import System.IO
import System.Directory
import System.FilePath

main :: IO ()
main = do
    withEnvCache limit spawner $ \cache ->
        forConcurrently_ [1..1000 :: Int] $ \n -> withEnv cache (\handle -> put handle n)
    where
        limit :: Limit
        limit = Hard 1

        put handle n = return ()

spawner :: Spawner Handle
spawner = Spawner
    { maker  = mkhandle
    , killer = hClose
    , isDead = hIsClosed
    }

counter = unsafePerformIO $ newIORef 0

mkhandle :: IO Handle
mkhandle = do
    c <- atomicModifyIORef' counter $ \c -> (c, c+1)
    openFile (show c) WriteMode

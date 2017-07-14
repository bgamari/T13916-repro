module Main where

import Bracket
import Control.Concurrent.STM 
import Control.Concurrent.Async
import Control.Concurrent
import System.IO
import System.Directory
import System.FilePath

__dir :: FilePath
__dir = "tmp"

main :: IO ()
main = main' __dir

main' :: FilePath -> IO ()
main' dir = do
    filenames <- newTVarIO []
    withEnvCache limit (spawner filenames dir) $ \cache ->
        forConcurrently_ [1..1000 :: Int] $ \n -> withEnv cache (\handle -> put handle n)
    where
        limit :: Limit
        limit = Hard 5

        put handle n = hPutStrLn handle (show n) *> threadDelay 10000

spawner :: TVar [FilePath] -> FilePath -> Spawner Handle
spawner filenames dir = Spawner
    { maker  = mkhandle filenames dir
    , killer = hClose
    , isDead = hIsClosed
    }

mkhandle :: TVar [FilePath] -> FilePath -> IO Handle
mkhandle filenames dir = do
    currentdir <- listDirectory dir
    newfilename <- findnewfilename currentdir (0 :: Int)
    openFile (dir </> newfilename) WriteMode
    where 
        findnewfilename currentdir n = do
            let fn = show n
            if fn `elem` currentdir then findnewfilename currentdir (succ n)
                else do
                    putsuccess <- atomically $ do
                                    alreadyput <- readTVar filenames
                                    if fn `elem` alreadyput then return False
                                        else modifyTVar' filenames (fn:) *> return True
                    if putsuccess then return fn else findnewfilename currentdir (succ n)

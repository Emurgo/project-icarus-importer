-- | Configuration of PostGres DB.

{-# LANGUAGE RankNTypes #-}

module Pos.BlockchainImporter.Configuration
       ( HasPostGresDB
       , withPostGresDB
       , withPostGreTransaction
       , maybePostGreStore
       ) where

import           Universum

import           Data.Reflection (Given (..), give, given)
import           Data.Word (Word64)
import qualified Database.PostgreSQL.Simple as PGS

type HasPostGresDB = Given PostGresDBConfiguration

data PostGresDBConfiguration = PostGresDBConfiguration
    {
      pgConnection :: !PGS.Connection
      -- ^ Connection to PostGres DB
    , pgStartBlock :: !Word64
      -- ^ Starting block number from which data will be stored on the DB
    }

withPostGreTransaction :: HasPostGresDB => IO a -> IO a
withPostGreTransaction = PGS.withTransaction (pgConnection given)

maybePostGreStore :: HasPostGresDB => Word64 -> (PGS.Connection -> IO ()) -> IO ()
maybePostGreStore currBN storeFn
  | currBN >= (pgStartBlock given)  = storeFn $ pgConnection given
  | otherwise                       = pure ()

withPostGresDB :: PGS.Connection -> Word64 -> (HasPostGresDB => r) -> r
withPostGresDB conn startBlock = give $ PostGresDBConfiguration conn startBlock

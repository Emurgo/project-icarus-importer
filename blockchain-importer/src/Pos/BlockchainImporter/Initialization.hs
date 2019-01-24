module Pos.BlockchainImporter.Initialization
  ( -- * Initialization functions
  initializePostgresDB
  ) where

import           Universum

import           System.Wlog (WithLogger, logInfo)

import           Pos.BlockchainImporter.Configuration (HasPostGresDB, postGreOperate)
import qualified Pos.BlockchainImporter.Tables.BestBlockTable as BestBlockT
import qualified Pos.BlockchainImporter.Tables.UtxosTable as UtxosT
import           Pos.Core (HasGenesisData)
import           Pos.Txp (genesisUtxo, unGenesisUtxo, utxoToModifier)

type InitializationMode m =
  ( MonadIO m
  , WithLogger m
  , HasPostGresDB
  , HasGenesisData
  )

{-
    Initializes postgres db to genesis block state, only when the db was previously not used:
    - Genesis UTxOs are store
    - Block number is set to 0
-}
initializePostgresDB :: InitializationMode m => m ()
initializePostgresDB = do
  postgresUtxos <- liftIO $ postGreOperate $ UtxosT.getUtxos
  let genesisUtxos = unGenesisUtxo genesisUtxo
  when (null postgresUtxos) $ do
    liftIO $ postGreOperate $ UtxosT.applyModifierToUtxos $ utxoToModifier genesisUtxos
    liftIO $ postGreOperate $ BestBlockT.updateBestBlock 0
    logInfo "Initialized postgres db"

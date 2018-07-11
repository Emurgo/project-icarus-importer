{-# LANGUAGE Arrows #-}

module Pos.BlockchainImporter.Tables.TxsTable
  ( -- * Data types
    TxRecord
    -- * Getters
  , getTxByHash
    -- * Manipulation
  , insertConfirmedTx
  , insertFailedTx
  , deleteTx
  ) where

import           Universum

import qualified Control.Arrow as A
import           Control.Lens (from)
import           Control.Monad (void)
import qualified Data.List.NonEmpty as NE (toList)
import           Data.Profunctor.Product.TH (makeAdaptorAndInstance)
import           Data.Time.Clock (UTCTime)
import qualified Database.PostgreSQL.Simple as PGS
import           Opaleye
import           Opaleye.RunSelect

import           Pos.BlockchainImporter.Core (TxExtra (..))
import           Pos.BlockchainImporter.Tables.TxAddrTable (TxAddrRowPGR, TxAddrRowPGW,
                                                            transactionAddrTable)
import qualified Pos.BlockchainImporter.Tables.TxAddrTable as TAT (insertTxAddresses)
import           Pos.BlockchainImporter.Tables.Utils
import           Pos.Core (Timestamp, timestampToUTCTimeL)
import           Pos.Core.Txp (Tx (..), TxId, TxOut (..), TxOutAux (..))
import           Pos.Crypto (hash)

data TxRecord = TxRecord
    { txHash            :: !TxId
    , txInputs          :: !(NonEmpty TxOutAux)
    , txOutputs         :: !(NonEmpty TxOutAux)
    , txBlockNum        :: !(Maybe Int64)
    , txFullProcessTime :: !(Maybe Timestamp)
    , txConfirmed       :: !Bool
    }

{-
  NOTE: The succeeded field can be obtained from checking whether the block number field is
        null or not (it is only null if the tx failed). It was left for making more clear whether
        a tx succeded or not.

  FIXME: Normalize the DB and delete the succeeded field, replacing it by a virtual one.
-}
data TxRowPoly h iAddrs iAmts oAddrs oAmts bn t succ = TxRow  { trHash          :: h
                                                              , trInputsAddr    :: iAddrs
                                                              , trInputsAmount  :: iAmts
                                                              , trOutputsAddr   :: oAddrs
                                                              , trOutputsAmount :: oAmts
                                                              , trBlockNum      :: bn
                                                              , trTime          :: t
                                                              , trSucceeded     :: succ
                                                              } deriving (Show)

type TxRowPGW = TxRowPoly (Column PGText)                   -- Tx hash
                          (Column (PGArray PGText))         -- Inputs addresses
                          (Column (PGArray PGInt8))         -- Inputs amounts
                          (Column (PGArray PGText))         -- Outputs addresses
                          (Column (PGArray PGInt8))         -- Outputs amounts
                          (Column (Nullable PGInt8))        -- Block number
                          (Column (Nullable PGTimestamptz)) -- Timestamp processing finished
                          (Column PGBool)                   -- Was successful

type TxRowPGR = TxRowPoly (Column PGText)                   -- Tx hash
                          (Column (PGArray PGText))         -- Inputs addresses
                          (Column (PGArray PGInt8))         -- Inputs amounts
                          (Column (PGArray PGText))         -- Outputs addresses
                          (Column (PGArray PGInt8))         -- Outputs amounts
                          (Column (Nullable PGInt8))        -- Block number
                          (Column (Nullable PGTimestamptz)) -- Timestamp processing finished
                          (Column PGBool)                   -- Was successful

$(makeAdaptorAndInstance "pTxs" ''TxRowPoly)

txsTable :: Table TxRowPGW TxRowPGR
txsTable = Table "txs" (pTxs TxRow  { trHash            = required "hash"
                                    , trInputsAddr      = required "inputs_address"
                                    , trInputsAmount    = required "inputs_amount"
                                    , trOutputsAddr     = required "outputs_address"
                                    , trOutputsAmount   = required "outputs_amount"
                                    , trBlockNum        = required "block_num"
                                    , trTime            = required "time"
                                    , trSucceeded       = required "succeeded"
                                    })

txAddrTable :: Table TxAddrRowPGW TxAddrRowPGR
txAddrTable = transactionAddrTable "tx_addresses"

insertConfirmedTx :: Tx -> TxExtra -> Word64 -> PGS.Connection -> IO ()
insertConfirmedTx tx txExtra blockHeight conn = insertTx tx txExtra (Just blockHeight) True conn

insertFailedTx :: Tx -> TxExtra -> PGS.Connection -> IO ()
insertFailedTx tx txExtra conn = insertTx tx txExtra Nothing False conn

-- | Inserts a given Tx into the Tx history tables.
insertTx :: Tx -> TxExtra -> Maybe Word64 -> Bool -> PGS.Connection -> IO ()
insertTx tx txExtra maybeBlockHeight succeeded conn = do
  insertTxToHistory tx txExtra maybeBlockHeight succeeded conn
  TAT.insertTxAddresses txAddrTable tx (teInputOutputs txExtra) conn

-- | Inserts the basic info of a given Tx into the master Tx history table.
insertTxToHistory :: Tx -> TxExtra -> Maybe Word64 -> Bool -> PGS.Connection -> IO ()
insertTxToHistory tx TxExtra{..} blockHeight succeeded conn = void $ runUpsert_ conn txsTable [row]
  where
    inputs  = toaOut <$> (catMaybes $ NE.toList $ teInputOutputs)
    outputs = NE.toList $ _txOutputs tx
    row = TxRow { trHash          = pgString $ hashToString (hash tx)
                , trInputsAddr    = pgArray (pgString . addressToString . txOutAddress) inputs
                , trInputsAmount  = pgArray (pgInt8 . coinToInt64 . txOutValue) inputs
                , trOutputsAddr   = pgArray (pgString . addressToString . txOutAddress) outputs
                , trOutputsAmount = pgArray (pgInt8 . coinToInt64 . txOutValue) outputs
                , trBlockNum      = fromMaybe (Opaleye.null) $
                                              (toNullable . pgInt8 . fromIntegral) <$> blockHeight
                  -- FIXME: Tx time should never be None at this stage
                , trTime          = maybeToNullable utcTime
                , trSucceeded     = pgBool succeeded
                }
    utcTime = pgUTCTime . (^. timestampToUTCTimeL) <$> teFullProcessTime

-- | Deletes a Tx by Tx hash from the Tx history tables.
deleteTx :: TxId -> PGS.Connection -> IO ()
deleteTx txId conn = void $ runDelete_  conn $
                                      Delete txsTable (\row -> trHash row .== txHash) rCount
  where
    txHash = pgString $ hashToString txId

-- | Returns a tx by hash
getTxByHash :: TxId -> PGS.Connection -> IO (Maybe TxRecord)
getTxByHash txHash conn = do
  txsMatched  :: [(Text, [Text], [Int64], [Text], [Int64], Maybe Int64, Maybe UTCTime, Bool)]
              <- runSelect conn txByHashQuery
  pure $ case txsMatched of
    [ ((_, inpAddrs, inpAmounts, outAddrs, outAmounts, blkNum, t, succeeded)) ] -> do
      inputs <- zipWithM toTxOutAux inpAddrs inpAmounts >>= nonEmpty
      outputs <- zipWithM toTxOutAux outAddrs outAmounts >>= nonEmpty
      let time = t <&> (^. from timestampToUTCTimeL)
      pure $ TxRecord txHash inputs outputs blkNum time succeeded
    _ -> Nothing
    where txByHashQuery = proc () -> do
            TxRow rowTxHash inputsAddr inputsAmount outputsAddr outputsAmount blkNum t succ <- (selectTable txsTable) -< ()
            restrict -< rowTxHash .== (pgString $ hashToString txHash)
            A.returnA -< (rowTxHash, inputsAddr, inputsAmount, outputsAddr, outputsAmount, blkNum, t, succ)

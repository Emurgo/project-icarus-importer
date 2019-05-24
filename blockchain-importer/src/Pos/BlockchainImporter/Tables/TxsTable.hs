{-# LANGUAGE Arrows #-}

module Pos.BlockchainImporter.Tables.TxsTable
  ( -- * Data types
    TxRecord (..)
  , TxState (..)
  , TxBlockData (..)
    -- * Getters
  , getTxByHash
    -- * Manipulation
  , upsertSuccessfulTx
  , upsertFailedTx
  , upsertPendingTx
  , markPendingTxsAsFailed
    -- * Recovery (use carefully!)
  , deleteTxsAfterBlk
  ) where

import           Universum

import qualified Control.Arrow as A
import           Control.Lens (from)
import           Control.Monad (void)
import qualified Data.List.NonEmpty as NE (toList)
import           Data.Profunctor.Product.TH (makeAdaptorAndInstance)
import           Data.Time.Clock (UTCTime, getCurrentTime)
import qualified Database.PostgreSQL.Simple as PGS
import           Opaleye
import           Opaleye.RunSelect

import           Pos.BlockchainImporter.Core (TxExtra (..))
import qualified Pos.BlockchainImporter.Tables.TxAddrTable as TAT (insertTxAddresses)
import           Pos.BlockchainImporter.Tables.Utils
import           Pos.Core (BlockCount, Timestamp, timestampToUTCTimeL, HeaderHash, SlotId (..))
import           Pos.Core.Txp (Tx (..), TxId, TxOut (..), TxOutAux (..), TxUndo)
import           Pos.Crypto (hash)
import           Pos.Crypto.Hashing (AbstractHash (..))
import           Pos.Binary.Class (serialize')
import           Serokell.Util.Base16 as SB16

-- txTimestamp corresponds to the trTimestamp
data TxRecord = TxRecord
    { txHash      :: !TxId
    , txInputs    :: !(NonEmpty TxOutAux)
    , txOutputs   :: !(NonEmpty TxOutAux)
    , txBlockNum  :: !(Maybe Int64)
    , txTimestamp :: !(Maybe Timestamp)
    , txState     :: !TxState
    }

{-|
    Tx state machine:
                          tx included
                            in block
                       --------------->
                      |                 Succesful
                      |   -------------     ^
                      |  |    block         |
                      |  |  rollbacked      |
        tx created    |  V                  |
      -------------> Pending                | Chain reorganization
                      ^  |                  |
                      |  | tx becomes       |
                      |  |   invalid        |
                      |   ------------->    |
                      |                  Failed
                       -----------------
                           tx resend
-}
data TxState  = Successful
              | Failed
              | Pending
              deriving (Show, Read)

{-|
    Given the possible events:
    - Tx gets confirmed (tx becomes Successful)
    - Tx sending fails (tx becomes Failed)
    - Tx gets created (tx becomes Pending)
    The difference between the trTimestamp and trLastUpdate is that:
    - trTimestamp is the time the event happened, that the tx changed it's state
    - trLastUpdate is the time the importer learned of this event

    The main case where both timestamp differ is when syncing from peers, trTimestamp is
    the moment that block was created (no matter how long ago this was) while trLastUpdate
    is the moment the importer received the block.

    Both are needed, as trTimestap is the one the user is interested of knowing, while
    trLastUpdate is used for fetching those events
-}
data TxRowPoly h iAddrs iAmts oAddrs oAmts bn t state last raw bhash epoch slot ord =
  TxRow
  { trHash          :: h
  , trInputsAddr    :: iAddrs
  , trInputsAmount  :: iAmts
  , trOutputsAddr   :: oAddrs
  , trOutputsAmount :: oAmts
  , trBlockNum      :: bn
  , trTimestamp     :: t
  , trState         :: state
  , trLastUpdate    :: last
  , trRawBody       :: raw
  , trBlockHash     :: bhash
  , trEpoch         :: epoch
  , trSlot          :: slot
  , trOrdinal       :: ord
  } deriving (Show)

type TxRowPG = TxRowPoly  (Column PGText)                   -- Tx hash
                          (Column (PGArray PGText))         -- Inputs addresses
                          (Column (PGArray PGInt8))         -- Inputs amounts
                          (Column (PGArray PGText))         -- Outputs addresses
                          (Column (PGArray PGInt8))         -- Outputs amounts
                          (Column (Nullable PGInt8))        -- Block number
                          (Column (Nullable PGTimestamptz)) -- Timestamp tx moved to current state
                          (Column PGText)                   -- Tx state
                          (Column PGTimestamptz)            -- Timestamp of the last update
                          (Column (Nullable PGText))        -- Raw TX body
                          (Column (Nullable PGText))        -- Block hash
                          (Column (Nullable PGInt4))        -- Epoch
                          (Column (Nullable PGInt4))        -- Slot
                          (Column (Nullable PGInt4))        -- Ordinal

$(makeAdaptorAndInstance "pTxs" ''TxRowPoly)

txsTable :: Table TxRowPG TxRowPG
txsTable = Table "txs" (pTxs TxRow  { trHash            = required "hash"
                                    , trInputsAddr      = required "inputs_address"
                                    , trInputsAmount    = required "inputs_amount"
                                    , trOutputsAddr     = required "outputs_address"
                                    , trOutputsAmount   = required "outputs_amount"
                                    , trBlockNum        = required "block_num"
                                    , trTimestamp       = required "time"
                                    , trState           = required "tx_state"
                                    , trLastUpdate      = required "last_update"
                                    , trRawBody         = required "tx_body"
                                    , trBlockHash       = required "block_hash"
                                    , trEpoch           = required "epoch"
                                    , trSlot            = required "slot"
                                    , trOrdinal         = required "ordinal"
                                    })


----------------------------------------------------------------------------
-- Getters and manipulation
----------------------------------------------------------------------------

-- | Returns a tx by hash
getTxByHash :: TxId -> PGS.Connection -> IO (Maybe TxRecord)
getTxByHash txHash conn = do
  txsMatched  :: [(Text, [Text], [Int64], [Text], [Int64], Maybe Int64, Maybe UTCTime, String)]
              <- runSelect conn txByHashQuery
  pure $ case txsMatched of
    [ (_, inpAddrs, inpAmounts, outAddrs, outAmounts, blkNum, t, txStateString) ] -> do
      inputs <- zipWithM toTxOutAux inpAddrs inpAmounts >>= nonEmpty
      outputs <- zipWithM toTxOutAux outAddrs outAmounts >>= nonEmpty
      txState <- readMaybe txStateString
      let time = t <&> (^. from timestampToUTCTimeL)
      pure $ TxRecord txHash inputs outputs blkNum time txState
    _ -> Nothing
    where txByHashQuery = proc () -> do
            TxRow rowTxHash inputsAddr inputsAmount outputsAddr outputsAmount blkNum t txState _ _ _ _ _ _ <- (selectTable txsTable) -< ()
            restrict -< rowTxHash .== pgString (hashToString txHash)
            A.returnA -< (rowTxHash, inputsAddr, inputsAmount, outputsAddr, outputsAmount, blkNum, t, txState)

-- | Returns slot from a latest known tx or (0/0)
getLatestSlot :: PGS.Connection -> IO SlotId
getLatestSlot conn = do
  slotsMatched :: [(Int, Int)] <- runSelect conn query
  pure $ case slotsMatched of
    [ (epoch, slot) ] -> createSlotId epoch slot
    _ -> createSlotId 0 0
  where query = proc () -> do
          (e,s) <- limit 1 (orderBy (desc fst <> desc snd) $ proc () -> do
            TxRow _ _ _ _ _ _ _ txState _ _ _ epoch slot _ <- selectTable txsTable -< ()
            restrict -< txState .== pgString (show Successful)
            A.returnA  -< (epoch, slot)) -< ()
          A.returnA -< (fromNullable 0 e, fromNullable 0 s)


{-|
    Inserts a confirmed tx to the tx history table
    If the tx was already present with a different state, it is moved to the confirmed one and
    it's timestamp and last update are updated.
-}
upsertSuccessfulTx :: Tx -> TxExtra -> TxBlockData -> PGS.Connection -> IO ()
upsertSuccessfulTx tx txExtra blockData =
   upsertTx tx txExtra (TxBlock blockData) Successful

{-|
    Inserts a failed tx to the tx history table with the current time as it's timestamp
    If the tx was already present with a different state, it is moved to the failed one and
    it's timestamp and last update are updated

    First argument is maybe of the slot this failed tx should be assigned to
    (usually current slot). If not available - latest known slot from DB
    will be taken, or 0/0.
-}
upsertFailedTx :: Maybe SlotId -> Tx -> TxUndo -> PGS.Connection -> IO ()
upsertFailedTx maybeSlot tx txUndo conn = do
  txExtra <- currentTxExtra txUndo
  slot <- maybe (getLatestSlot conn) pure maybeSlot
  upsertTx tx txExtra (TxSlot slot) Failed conn

{-|
    Inserts a pending tx to the tx history table with the current time as it's timestamp
    If the tx was already present with a different state, it is moved to the pending one and
    it's timestamp and last update are updated
-}
upsertPendingTx :: Tx -> TxUndo -> PGS.Connection -> IO ()
upsertPendingTx tx txUndo conn = do
  txExtra <- currentTxExtra txUndo
  upsertTx tx txExtra TxUnchained Pending conn

{-|
    Marks all pending txs as failed
    All the txs changed have their timestamp and last update changed to the current time
-}
markPendingTxsAsFailed :: PGS.Connection -> IO ()
markPendingTxsAsFailed conn = do
  currentTime <- getCurrentTime
  let changePendingToFailed row = row
                                    { trState      = show Failed
                                    , trTimestamp  = toNullable $ pgUTCTime currentTime
                                    , trLastUpdate = pgUTCTime currentTime
                                    }
      isPending row             = trState row .== show Pending
  void $ runUpdate_ conn $ Update txsTable changePendingToFailed isPending rCount

{-|
    As pending and failed txs have a NULL block number, this deletes all successful txs
    whose block number is greater than the provided one

    As this is called on start of the importer, all pending txs will have been moved to
    failed with 'markPendingTxsAsFailed'. If any of these was successful, it will eventually
    be moved to that state on syncing.
-}
deleteTxsAfterBlk :: BlockCount -> PGS.Connection -> IO ()
deleteTxsAfterBlk fromBlk conn = void $ runDelete_ conn deleteAfterBlkQuery
  where deleteAfterBlkQuery = Delete txsTable shouldDeleteTx rCount
        shouldDeleteTx tx   = matchNullable (pgBool False)
                                            (\txBlkNum -> txBlkNum .> pgInt8 (fromIntegral fromBlk))
                                            (trBlockNum tx)

----------------------------------------------------------------------------
-- Helpers
----------------------------------------------------------------------------

-- Inserts a given Tx into the Tx history tables with a given state
-- (overriding any if it was already present).
upsertTx :: Tx -> TxExtra -> TxChainData -> TxState -> PGS.Connection -> IO ()
upsertTx tx txExtra chainData succeeded conn = do
  upsertTxToHistory tx txExtra chainData succeeded conn
  TAT.insertTxAddresses tx (teInputOutputs txExtra) conn

-- Inserts the basic info of a given Tx into the master Tx history table
-- (overriding any if it was already present)
upsertTxToHistory :: Tx -> TxExtra -> TxChainData -> TxState -> PGS.Connection -> IO ()
upsertTxToHistory tx TxExtra{..} chainData txState conn = do
  currentTime <- getCurrentTime
  void $ runUpsert_ conn txsTable ["hash"]
                    ["block_num", "block_hash", "tx_state", "last_update",
                     "time", "epoch", "slot", "ordinal"]
                    [rowFromLastUpdate currentTime]
  where
    inputs                        = toaOut <$> catMaybes (NE.toList teInputOutputs)
    outputs                       = NE.toList $ _txOutputs tx
    (mSlot, mBlHeight, mBlHash, mTxOrd) = extractChainData chainData
    rowFromLastUpdate currentTime =
          TxRow { trHash          = pgString $ hashToString (hash tx)
                , trInputsAddr    = pgArray (pgString . addressToString . txOutAddress) inputs
                , trInputsAmount  = pgArray (pgInt8 . coinToInt64 . txOutValue) inputs
                , trOutputsAddr   = pgArray (pgString . addressToString . txOutAddress) outputs
                , trOutputsAmount = pgArray (pgInt8 . coinToInt64 . txOutValue) outputs
                , trBlockNum      = maybeOrNull (pgInt8 . fromIntegral) mBlHeight
                , trBlockHash     = maybeOrNull (pgString . extractHash) mBlHash
                  -- FIXME: Tx time should never be None at this stage
                , trTimestamp     = maybeToNullable $ timestampToPGTime <$> teTimestamp
                , trState         = pgString $ show txState
                , trLastUpdate    = pgUTCTime currentTime
                , trRawBody       = toNullable . pgString. toString . serialize' $ tx
                , trEpoch         = maybeOrNull (pgInt4 . slotToEpochInt) mSlot
                , trSlot          = maybeOrNull (pgInt4 . slotToSlotInt) mSlot
                , trOrdinal       = maybeOrNull pgInt4 mTxOrd
                }
    timestampToPGTime = pgUTCTime . (^. timestampToUTCTimeL)

currentTxExtra :: TxUndo -> IO TxExtra
currentTxExtra txUndo = do
  currentTime <- getCurrentTime
  let currentTimestamp = currentTime ^. from timestampToUTCTimeL
  pure $ TxExtra (Just currentTimestamp) txUndo

-- | A required instance for decoding.
instance ToString ByteString where
  toString = toString . SB16.encode

-- | Extracting actual digest from type-wrapper
extractHash :: HeaderHash -> String
extractHash (AbstractHash h) = show h

-- | Map a maybe value into a nullable column
maybeOrNull :: (a -> Column b) -> Maybe a -> Column (Nullable b)
maybeOrNull f = maybe Opaleye.null (toNullable . f)

-- | Possible states of a tx relevance to the chain
data TxChainData
   = TxSlot SlotId -- tx is pegged to some slot
   | TxBlock TxBlockData  -- tx is included in a block
   | TxUnchained -- tx is not related to the chain

-- | Extended properties of a tx being included in a block
data TxBlockData
   = TxBlockData { slot        :: !SlotId
                 , blockHeight :: !BlockCount
                 , blockHash   :: !HeaderHash
                 , txOrdinal   :: !Int
                 }

extractChainData :: TxChainData -> (Maybe SlotId, Maybe BlockCount, Maybe HeaderHash, Maybe Int)
extractChainData maybeChainData = case maybeChainData of
    TxSlot slot -> (Just slot, Nothing, Nothing, Nothing)
    TxBlock TxBlockData{..} -> (Just slot, Just blockHeight, Just blockHash, Just txOrdinal)
    _ -> (Nothing, Nothing, Nothing, Nothing)

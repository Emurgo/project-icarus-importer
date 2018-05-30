{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE RankNTypes          #-}

-- | Blockchain listener for BlockchainImporter.
-- Callbacks on application and rollback.

module Pos.BlockchainImporter.BListener
       ( runBlockchainImporterBListener
       , BlockchainImporterBListener
       -- * Instances
       -- ** MonadBListener (BlockchainImporterBListener m)
       -- * Required for migration
       , findEpochMaxPages
       ) where

import           Nub (ordNub)
import           Universum

import           Control.Lens (at, non)
import           Control.Monad.Trans.Identity (IdentityT (..))
import           Data.Coerce (coerce)
import           Data.List ((\\))
import qualified Data.List.NonEmpty as NE
import qualified Data.Map as M
import qualified Ether
import           System.Wlog (WithLogger)
import           UnliftIO (MonadUnliftIO)

import           Pos.Block.BListener (MonadBListener (..))
import           Pos.Block.Types (Blund)
import           Pos.BlockchainImporter.DB (Epoch, EpochPagedBlocksKey, Page, defaultPageSize,
                                            findEpochMaxPages, numOfLastTxs)
import qualified Pos.BlockchainImporter.DB as DB
import           Pos.Core (HasConfiguration, HeaderHash, LocalSlotIndex (..), SlotId (..),
                           difficultyL, epochIndexL, getChainDifficulty, headerHash, mainBlockSlot)
import           Pos.Core.Block (Block, MainBlock, mainBlockTxPayload)
import           Pos.Core.Txp (Tx, txpTxs)
import           Pos.Crypto (withHash)
import           Pos.DB.BatchOp (SomeBatchOp (..))
import           Pos.DB.Class (MonadDBRead)
import           Pos.Txp (topsortTxs)
import           Pos.Util.AssertMode (inAssertMode)
import           Pos.Util.Chrono (NE, NewestFirst (..), OldestFirst (..), toNewestFirst)


----------------------------------------------------------------------------
-- Declarations
----------------------------------------------------------------------------


data BlockchainImporterBListenerTag

type BlockchainImporterBListener = Ether.TaggedTrans BlockchainImporterBListenerTag IdentityT

-- Unwrap the @BListener@.
runBlockchainImporterBListener :: BlockchainImporterBListener m a -> m a
runBlockchainImporterBListener = coerce

-- Type alias, remove duplication
type MonadBListenerT m =
    ( WithLogger m
    , MonadCatch m
    , MonadDBRead m
    , MonadUnliftIO m
    , HasConfiguration
    )

-- BlockchainImporter implementation for usual node. Combines the operations.
instance ( MonadDBRead m
         , MonadUnliftIO m
         , MonadCatch m
         , WithLogger m
         , HasConfiguration
         )
         => MonadBListener (BlockchainImporterBListener m) where
    onApplyBlocks     blunds = onApplyCallGeneral blunds
    onRollbackBlocks  blunds = onRollbackCallGeneral blunds


----------------------------------------------------------------------------
-- General calls
----------------------------------------------------------------------------


onApplyCallGeneral
    :: MonadBListenerT m
    => OldestFirst NE Blund
    -> m SomeBatchOp
onApplyCallGeneral    _ = do
    inAssertMode DB.sanityCheckBalances
    pure mempty


onRollbackCallGeneral
    :: MonadBListenerT m
    => NewestFirst NE Blund
    -> m SomeBatchOp
onRollbackCallGeneral _ = do
    inAssertMode DB.sanityCheckBalances
    pure mempty

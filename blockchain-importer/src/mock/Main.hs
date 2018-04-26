-- | This program runs a simple data mock of the BlockchainImporter API.

module Main
    ( main
    ) where

import           Universum

import           Data.Version (showVersion)
import           Options.Applicative (execParser, footer, fullDesc, header, help, helper, info,
                                      infoOption, long, progDesc)

import qualified Paths_cardano_sl_blockchain_importer as CSLE
import           Pos.BlockchainImporter.Web.TestServer (runMockServer)



main :: IO ()
main = do
    showProgramInfoIfRequired
    runMockServer
  where
    -- | Showing info for the program.
    showProgramInfoIfRequired :: IO ()
    showProgramInfoIfRequired = void $ execParser programInfo
      where
        programInfo = info (helper <*> versionOption) $
            fullDesc <> progDesc "Run mock for BlockchainImporter web API."
                     <> header   "Cardano SL BlockchainImporter web mock."
                     <> footer   ("This program returns just the mocked data. " <>
                                  "It doesn't call any CSL functions and doesn't interact with it. " <>
                                  "It just implements the API and returns simple test data.")

        versionOption = infoOption
            ("cardano-mock-" <> showVersion CSLE.version)
            (long "version" <> help "Show version.")

# `cardano-sl-blockchain-importer`

## Installation

### Requirements

Installation of `nix` is needed.

```bash
curl https://nixos.org/nix/install | sh
source ~/.nix-profile/etc/profile.d/nix.sh
```

Make sure that `nix` is set to `true` within `~/.stack/config.yaml`.

```
nix:
  enable: true
```

## Generate documentation

Generated documentation for BlockchainImporter Web API is available [online](https://cardanodocs.com/technical/blockchain-importer/api/).

## Run mock server

```bash
stack exec cardano-blockchain-importer-mock
```

## Run it

Run it from project root.

### Dev version

- run `./scripts/build/cardano-sl.sh`
- run `./scripts/launch/blockchain-importer-with-nodes.sh`

### Prod version (connects BlockchainImporter to `staging` or `mainnet`)

- Run `/scripts/clean/db.sh` to do a clean synchronization, so that BlockchainImporter will sync and download blockchain from start.
- Connect to cluster as described in  `docs/how-to/connect-to-cluster.md`
- Open http://localhost:3100/ in your browser. (Note: It takes some time to sync all data from cluster. That's why BlockchainImporter's UI might not display latest data from start.)


## Sockets

`CORS` requests to connect `socket` server are currently restricted to following resources:
* https://cardano-blockchain-importer.com
* https://blockchain-importer.iohkdev.io
* http://cardano-blockchain-importer.cardano-mainnet.iohk.io
* http://localhost:3100

Change `CORS` policies in `src/Pos/BlockchainImporter/Socket/App.hs` whenever you have to add more resources.

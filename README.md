# bee herder 🐝📢

This is a `CLI` utility to asist with handling / co-ordinating large data uploads to the Ethereum Swarm. This utility has been created for the purposes of the FDS Wikipedia Gitcoin bounty. This utility is designed to handle:

1. Arbitrary folder importing to the utility's internal database.
2. Upload of files to the `/bytes` API endpoint of Ethereum Swarm with tagging and local pinning.
3. Parallel mantaray manifest generation and upload to the `/bytes` API endpoint.

## Folder import

To import a folder into the internal database:

```bash
bee_herder import --db /path/to/db /path/of/folder/to/import
```

This will parse the folder, recursively importing these files into the utility's internal database. This does **NOT** import the file's contents into the utilty's database - it will create an index entry and record the metadata for the file based on the file's suffix. The utility stores the *absolute* path to the file that is to be uploaded, therefore it is important not to move the contents prior to `upload`. For more information on the `import` subcommand, see `bee_herder import --help`.

## Uploading

To upload the contents to the target Bee node:

```bash
bee_herder upload --db /path/to/db http://127.0.0.1:1633 <postage_batch_id> <target upload rate in files / sec>
```

The upload will:
    - Generate tags (a batch of 100) and then split the files amongst the 100 tags.
    - Upload the files to the `/bytes` API endpoint. 
    - Record the returned reference in the internal database.

## Manifests

`bee_herder` makes use of `mantaray-rs`, a Rust-based implementation of the `mantaray` manifest data structure in use on Ethereum Swarm. Use overview:

```bash
./bee_herder manifest --db /path/to/db http://127.0.0.1:1633 <postage_batch_id> <batch_size> <upload_count> <channels> <hints>
```

Example use of the above is as follows:

```bash
./bee_herder manifest --db /path/to/db http://bee.swarm.public.dappnode:1633 fe4203d5ebcef8bc8810579eb95989454ec9eb554e916a75ac6ebafc72b8dac7 1500 8 50 "wiki/" "media/"
```

The above command achieves:

1. Use the utility's database at `/path/to/db`.
2. Upload the generated manifest to the API at `http://bee.swarm.public.dappnode:1633` using the postage stamp `fe4203d5ebcef8bc8810579eb95989454ec9eb554e916a75ac6ebafc72b8dac7`.
3. For each worker thread, use a batch size of `1500` nodes before uploading and pruning the forks to save on memory usage.
4. Use a maximum of `8` concurrent threads (`upload_count`) and a buffer of `50` **batches** (`channels`) when processing the manifest.

Upon successful completion, the command will then return the reference of the mantaray manifest's root node.

**WARNING: Very careful consideration must to be given to the `batch_size`, `upload_count` and `channels` as incorrect tuning can easily result in memory / file descriptor exhaustion.**

### Manifest Merging

One very handy feature available with `bee_herder` in the `manifest` subcommand is the ability to merge manifests. The use case is as follows:

1. Upload the entire Wikipedia, _articles / media_ only into the Swarm and get the manifest root reference.
2. Create a new frontend (with a separate manifest database):
    a. `import` the `dist` folder of the respective frontend once it's been generated.
    b. `upload` the frontend to the Swarm.
    c. Generate the `manifest`, with the `--merge-manifest` option, eg:

        ```bash
        bee_herder manifest --db /path/to/db --merge-manifest cc73d85db493df9c33174be9f72d16da91d9da7f4b50b30028d3e56bd022c216 http://bee.swarm.public.dappnode:1633 fe4203d5ebcef8bc8810579eb95989454ec9eb554e916a75ac6ebafc72b8dac7 1500 8 50
        ```

This will create the new manifest, starting from the manifest root of `cc73d85db493df9c33174be9f72d16da91d9da7f4b50b30028d3e56bd022c216`, and subsequently build out a new manifest with the files imported, returning a new manifest root. This helps substantially for selective file replacement / upgrade of a manifest's contents.

# Test case

A DAppNode Extreme Edition (Intel(R) Core(TM) i7-10710U CPU 6 Core, 32GB RAM, 4TB NVME SSD) was used to process the full English Wikipedia.

- Extraction / Parsing: ~2 hrs
- Uploading: ~2 days
- Manifest generation / uploading: 15 hrs

# Todo

- [ ] Better documentation
- [ ] Monitoring of chunk syncing (polling `/tags` endpoint)
- [ ] Stewardship of individual files and manifest nodes
- [ ] `feed` generation and updating
- [ ] Optionally include additional metadata (eg. file modification) when importing
- [ ] Local generation of `mantaray` manifest (including `chunks` before upload) to minimise wastage (pruned forks that do not make it into the final manifest).

# Contributions

A big thank you to the team at Ethereum Swarm for answering of questions during the development of this utility. An additional thank you to @ldeffenb who has provided invaluable guidance from their experience with marshalling OpenStreetMap datasets into Swarm.
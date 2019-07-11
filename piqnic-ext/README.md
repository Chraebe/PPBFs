Note: This is an extension of the original PIQNIC repository, version 1.0-SNAPSHOT, and as with that version, it was made for experimental evaluation. The original repository is available at https://github.com/Chraebe/PIQNIC. Please see the file readme_PIQNIC-Ext for an overview over created and changed files for the extension.

# PIQNIC: a P2p system for Query processiNg over semantIC data
A github repository for PIQNIC Version 0.1-SNAPSHOT

This Readme and repository are works in progress. The PIQNIC version available here is the one used for experiments in our paper (see also our website at http://relweb.cs.aau.dk/piqnic/).

### Installation
1. Download the sources and unpack.
2. Use `mvn install` to generate jar file
3. Run with the following command: java -jar [filename].jar [config].json:
4. Connect to the CLI port and follow the steps on screen.
5. Example config.json:
```
   {
     "replication" : 4,
     "ports" : {
       "listener" : 7625,
       "cli" : 7626,
       "test" : 7627
     "maxFragments" : 200,
     "timeToLive" : 5,
     "shuffleLength" : 4,
     "neighbours" : 5,
     "minutesTilShuffle" : 200,
     "maxDelay" : 500,
     "version" : "0.1-SNAPSHOT"
   }
   ```
   
### Test Startup
Note: For the extension, the directory also needs .ppbf and .ppbfp files.

We have included the ability to quickly setup a test-environment. The requirements for this, however, is that the dataset is already distributed to the clients in the following directory format (the dataet and distribution we used can be downloaded on our website at https://relweb.cs.aau.dk/piqnic/):

  -  dataset1
      - fragment1.hdt
      - fragment1.hdt.ppbf
      - fragment1.hdt.ppbfp
      - fragment2.hdt
      - fragment2.hdt.ppbf
      - fragment2.hdt.ppbfp
      - fragment3.hdt
      - fragment3.hdt.ppbf
      - fragment3.hdt.ppbfp
      - fragments
    - dataset2
      - fragment1.hdt
      - fragment2.hdt
      - fragment3.hdt
      - fragment1.hdt
      - fragment1.hdt.ppbf
      - fragment1.hdt.ppbfp
      - fragment2.hdt
      - fragment2.hdt.ppbf
      - fragment2.hdt.ppbfp
      - fragment3.hdt
      - fragment3.hdt.ppbf
      - fragment3.hdt.ppbfp
      - fragments
    - dataset_distro

The file `dataset_distro` contains each dataset and the client it has been distributed to, in the following format: `dataset1;125`, means that dataset1 has been assigned to peer 125, and is owned by that. One line per dataset (keep the file names as fragments and dataset_distro). Index files are optional for HDT files.

The clients can then be started with the included `test_startup.sh` file like so:

```
./test_startup.sh [filename].jar [config].json [Client no.] [No. of Clients] [Replication Factor] [No. of Neighbors] [/path/to/datasets/]
```

Each fragment directory has the `fragments` file, containing the URI of the predicates in the fragment and its identifier, e.g. `http://www.w3.org/1999/02/22-rdf-syntax-ns#type;fragment1`

---   
Any questions, feel free to contact Christian Aebeloe at caebel@cs.aau.dk

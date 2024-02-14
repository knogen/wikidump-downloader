wikipeida pages-meta-history1.xml download


# useage 

WikiDump Downloader

options:
  -h, --help            show this help message and exit
  --data-path DATA_PATH
                        the data directory
  --proxies PROXIES     use the downloader proxies
  --compress-type COMPRESS_TYPE
                        the compressed file type to download: 7z or bz2 [default: 7z]
  --threads THREADS     number of threads [default: 3]
  --start START         the first file to download [default: 0]
  --end END             the last file to download [default: -1]
  --verify              verify the dump files in the specific path

# my useage 

`export PATH=$PATH:/home/ider/.local/bin && cd /home/ider/workspace/wikidump-downloader && /usr/bin/make start` 

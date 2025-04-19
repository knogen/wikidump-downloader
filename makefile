
download: 
	uv run  python -m wikidump_downloader --data-path /mnt/st01/wikipeida_download --proxies http://192.168.1.230:10808
	
verify: 
	uv run python -m wikidump_downloader --data-path /mnt/st01/wikipeida_download --verify

start: download verify

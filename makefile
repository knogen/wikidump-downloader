init:
	poetry install

download: init
	poetry run start --data-path /mnt/st01/wikipeida_download --proxies http://192.168.1.230:10809
	
verify: init
	poetry run start --data-path /mnt/st01/wikipeida_download --verify

start: download verify

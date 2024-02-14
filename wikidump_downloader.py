import argparse
import glob
import hashlib
import json
import logging
import os
import threading
import urllib.request
from datetime import datetime
import pathlib

parser = argparse.ArgumentParser(description='WikiDump Downloader')
parser.add_argument('--data-path', type=str, default="./data/", help='the data directory')
parser.add_argument('--proxies', type=str, default="", help='use the downloader proxies')
parser.add_argument('--compress-type', type=str, default='7z',
                    help='the compressed file type to download: 7z or bz2 [default: 7z]')
parser.add_argument('--threads', type=int, default=3, help='number of threads [default: 3]')
parser.add_argument('--start', type=int, default=1, help='the first file to download [default: 0]')
parser.add_argument('--end', type=int, default=-1, help='the last file to download [default: -1]')
parser.add_argument('--verify', action='store_true', default=False, help='verify the dump files in the specific path')
args = parser.parse_args()

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)s) %(message)s',
                    )

WIKIMEDIA_MIRRORS = ["https://dumps.wikimedia.org", 
                     "https://wikimedia.bringyour.com", 
                     "https://wikipedia.c3sl.ufpr.br", 
                     "https://mirror.clarkson.edu", 
                     "https://wikimedia.mirror.clarkson.edu", 
                     "https://wikipedia.mirror.pdapps.org"]


class wikipediaMirror:
    def __init__(self):
        self.index = 0
        
    def get_mirror(self):
        
        if self.index<len(WIKIMEDIA_MIRRORS)-1:
            self.index += 1
        else:
            self.index = 0
        return WIKIMEDIA_MIRRORS[self.index]
        
WM = wikipediaMirror()

def download(dump_status_file, data_path, compress_type, start, end, thread_num):
    url_list = []
    file_list = []
    with open(dump_status_file) as json_data:
        # Two dump types: compressed by 7z (metahistory7zdump) or bz2 (metahistorybz2dump)
        history_dump = json.load(json_data)['jobs']['metahistory' + compress_type + 'dump']
        dump_dict = history_dump['files']
        dump_files = sorted(list(dump_dict.keys()))

        if args.end > 0 and args.end <= len(dump_files):
            dump_files = dump_files[start - 1:end]
        else:
            dump_files = dump_files[start - 1:]

        # print all files to be downloaded.
        print("All files to download ...")
        for i, file in enumerate(dump_files):
            print(i + args.start, file)

        file_num = 0
        for dump_file in dump_files:
            file_name = data_path.joinpath(dump_file)
            file_list.append(file_name)

            # url example: https://dumps.wikimedia.org/enwiki/20180501/enwiki-20180501-pages-meta-history1.xml-p10p2123.7z
            # url = "https://dumps.wikimedia.org" + dump_dict[dump_file]['url']
            url = dump_dict[dump_file]['url']
            url_list.append(url)
            file_num += 1

        print('Total file ', file_num, ' to be downloaded ...')
        json_data.close()

    task = WikiDumpTask(file_list, url_list)
    threads = []
    for i in range(thread_num):
        t = threading.Thread(target=worker, args=(i, task))
        threads.append(t)
        t.start()

    logging.debug('Waiting for worker threads')
    main_thread = threading.current_thread()
    for t in threading.enumerate():
        if t is not main_thread:
            t.join()

def md5(file):
    hash_md5 = hashlib.md5()
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(40960000), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def verify(dump_status_file, compress_type, data_path:pathlib.Path):
    print("Verify the file in folder:", data_path)
    pass_files, miss_files, crash_files = [], [], []
    with open(dump_status_file) as json_data:
        # Two dump types: compressed by 7z (metahistory7zdump) or bz2 (metahistorybz2dump)
        history_dump = json.load(json_data)['jobs']['metahistory' + compress_type + 'dump']
        dump_dict = history_dump['files']
        for i, (file, value) in enumerate(dump_dict.items()):
            gt_md5 = value['md5']
            print("#", i, " ", file, ' ', value['md5'], sep='')
            file_path = data_path.joinpath(file)
            if file_path.exists():
                file_md5 = md5(file_path)
                if file_md5 == gt_md5:
                    pass_files.append(file)
                else:
                    crash_files.append(file)
                    os.remove(file_path)
            else:
                miss_files.append(file)

    print(len(pass_files), "files passed, ", len(miss_files), "files missed, ", len(crash_files), "files crashed.")

    if len(miss_files):
        print("==== Missed Files ====")
        print(miss_files)

    if len(crash_files):
        print("==== Crashed Files ====")
        print(crash_files)
    
    if len(miss_files) == 0 and len(crash_files) == 0:
        return True
    return False


def main():
    now = datetime.now()
    version_flag = now.strftime('%Y%m01')
    
    data_path = pathlib.Path(args.data_path).joinpath(version_flag)
    data_path.mkdir(exist_ok=True)
    
    logging.info("start version: %s, data path: %s",version_flag, data_path)
    
    dump_status_file = data_path.joinpath("dumpstatus.json")
    if not dump_status_file.exists():
        urllib.request.urlretrieve(f"https://dumps.wikimedia.org/enwiki/{version_flag}/dumpstatus.json", dump_status_file)
        
    space_stats_flag = data_path.joinpath("COMPLETE.txt")
    if space_stats_flag.exists():
        with open(space_stats_flag,'rt')as f:
            data = f.read()
        logging.info("download complete as: %s",data)
        return
        
    if args.verify:
        result = verify(dump_status_file, args.compress_type, data_path)
        if result:
            with open(space_stats_flag,'wt')as f:
                f.write(str(datetime.now())) 
    else:
        download(dump_status_file, data_path, args.compress_type, args.start, args.end, args.threads)


'''
WikiDumpTask class contains a list of dump files to be downloaded . 
The assign_task function will be called by workers to grab a task.
'''


class WikiDumpTask(object):
    def __init__(self, file_list, url_list):
        self.lock = threading.Lock()
        self.url_list = url_list
        self.file_list = file_list
        self.total_num = len(url_list)

    def assign_task(self):
        logging.debug('Assign tasks ... Waiting for lock')
        self.lock.acquire()
        url = None
        file_name = None
        cur_progress = None
        try:
            # logging.debug('Acquired lock')
            if len(self.url_list) > 0:
                url = self.url_list.pop(0)
                file_name = self.file_list.pop(0)
                cur_progress = self.total_num - len(self.url_list)
        finally:
            self.lock.release()
        return url, file_name, cur_progress, self.total_num


'''
worker is main function for each thread.
'''


def worker(work_id, tasks):
    logging.debug('Starting.')
    if args.proxies:
        
        proxy_setting = {'http': args.proxies, 'https': args.proxies}
        proxy_handler = urllib.request.ProxyHandler(proxy_setting)
        opener = urllib.request.build_opener(proxy_handler)
    # Install the opener as the default opener
    urllib.request.install_opener(opener)

    # grab one task from task_list
    while 1:
        url, file_name, cur_progress, total_num = tasks.assign_task()
        if not url:
            break
        logging.debug('Assigned task (' + str(cur_progress) + '/' + str(total_num) + '): ' + str(url))

        if not file_name.exists():
            while 1:
                try:
                    dump_url = WM.get_mirror() + url
                    logging.debug("start get file: " + dump_url)
                    urllib.request.urlretrieve(dump_url, file_name)
                    logging.debug("File Downloaded: " + url)
                    break
                except Exception as e:
                    logging.warning("download fiale, retry: %s", e)
                    
        else:
            logging.debug("File Exists, Skip: " + url)
    logging.debug('Exiting.')

    return


if __name__ == '__main__':
    start_time = datetime.now()
    main()
    time_elapsed = datetime.now() - start_time
    print('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))
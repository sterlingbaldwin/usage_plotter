import os
import sys
import argparse
from tqdm import tqdm
from pathlib import Path
from ipwhois import IPWhois
import itertools
from pprint import pprint


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('root', help="path to directory full of access logs for ESGF datasets")
    return parser.parse_args()

def get_logs(path):
    for root, dirs, files in os.walk(path):
        if not files:
            continue
        if dirs:
            continue
        for file in files:
            yield str(Path(root, file).absolute())

def filter_lines(path):
    with open(path, 'r') as instream:
        while (line := instream.readline()):
            if "E3SM" in line \
            and "CMIP6" not in line \
            and "xml" not in line \
            and "ico" not in line \
            and "cmip6_variables" not in line \
            and 'html' not in line \
            and "catalog" not in line \
            and "aggregation" not in line:
                yield line


REALMS = ['ocean', 'atmos', 'land', 'sea-ice']
DATA_TYPES = ['time-series', 'climo', 'model-output', 'mapping', 'restart']


def plot_e3sm_project(datasets):
    import matplotlib
    import matplotlib.pyplot as plt
    # %matplotlib inline
    plt.style.use('ggplot')
    
    keys = sorted(list(datasets.keys()))
    x_pos = [i for i, _ in enumerate(keys)]
    values = []
    for key in keys:
        values.append(datasets[key]['requests'])
    
    plt.bar(x_pos, values, width=0.35)
    
    key_names = [f"{k}:{v}" for k, v in keys]
    plt.xticks(x_pos, key_names, rotation = 30, ha='right')

    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(18.5, 10.5)
    
    plt.show()

def main():
    parsed_args = parse_args()
    root = parsed_args.root

    datasets = {
        x: {'requests': 0} for x in itertools.product(REALMS, DATA_TYPES)
    }

    for log in tqdm(get_logs(root)):
        for line in filter_lines(log):
            attrs = line.split()
            status = attrs[8]
            # import ipdb; ipdb.set_trace()
            if "20" not in status:
                continue

            fullpath = attrs[6]
            requester = attrs[0]
            try:
                idx = fullpath.index('user_pub_work') + len('user_pub_work') + 1
            except:
                print("ERROR: " + fullpath)
                continue
            dataset_id = ".".join(fullpath[idx:].split('/')[:-1])

            try:
                facets = dataset_id.split('.')
                realm = facets[4]
                data_type = facets[6]
                # import ipdb; ipdb.set_trace()
                tag = f"{requester}:{dataset_id}"
                if datasets[(realm, data_type)].get(tag) is None:
                    datasets[(realm, data_type)]['requests'] += 1
                    datasets[(realm, data_type)][tag] = 1
                else:
                    datasets[(realm, data_type)][tag] += 1
                
            except Exception as e:
                pass
                # print(e)
                # import ipdb; ipdb.set_trace()
                # ...
    
    
    # import ipdb; ipdb.set_trace()
    # pprint(datasets)
    plot_e3sm_project(datasets)


    return 0

if __name__ == "__main__":
    sys.exit(main())
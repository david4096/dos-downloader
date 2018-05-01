import argparse
import requests
import sys

from ga4gh.dos.client import Client

def download_file(url, filename):
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            sys.stdout.write(".")
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
    print('done')
    return filename




def download(url, id):
    """Download data from a DOS service."""
    local_client = Client(url)
    client = local_client.client
    models = local_client.models
    data_object = client.GetDataObject(data_object_id=id).result().data_object
    return download_file(data_object.urls[0].url, data_object.name)

def main(args=None):
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('url',
                        help='The location of the Data Object Service')
    parser.add_argument('id',
                        help='The identifier of the Data Object')
    args = parser.parse_args(args)
    download(args.url, args.id)


if __name__ == '__main__':
    main()

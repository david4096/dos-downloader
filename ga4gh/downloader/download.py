import argparse
import requests
import sys

from urlparse import urlparse

from ga4gh.dos.client import Client
from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver


def download_s3(api_key, secret_key, container, key, path, region='us-west-2'):
    """
    Downloads from an S3 url using libcloud

    :param api_key:
    :param secret_key:
    :return:
    """
    cls = get_driver(Provider.S3)
    if region:
        driver = cls(api_key, secret_key, region=region)
    else:
        driver = cls(api_key, secret_key)
    obj = driver.get_object(container_name=container,
                            object_name=key)

    if obj.download(destination_path=path, overwrite_existing=True):
        print('Done downloading')
        return path
    else:
        return None


def download_file(url, filename):
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            sys.stdout.write(".")
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    print('Done downloading.')
    return filename


def handle_url(url, path, aws_access_key, aws_secret_key):
    """
    Handles a URL from the urls on a Data Object to attempt a download.

    :param url:
    :param aws_access_key:
    :param aws_secret_key:
    :return:
    """
    if url.url.find('s3://') != -1:
        parsed = urlparse(url.url)
        # the first part of the path is the container
        container = parsed.netloc.split('.')[0]
        # may need to remove s3
        # the key is the rest
        key = '/'.join(parsed.path.split('/')[1:])
        return download_s3(aws_access_key,
                           aws_secret_key,
                           container,
                           key,
                           path,
                           region='us-west-2')
    else:
        return download_file(url, path)


def pick_url(urls, max):
    """
    CLI interface function for selecting the download URL.

    :param urls:
    :return:
    """
    print(max)
    print('More than one URL was found for the Data Object')
    print('Please select a URL from the list: ')
    print("\n".join(["[{}] {}".format(i, x.url) for i, x in (
        enumerate(urls))]))
    choice = raw_input()
    if choice is None or int(choice) < 0 or int(choice) > max:
        return pick_url(urls, len(urls))
    else:
        print('Downloading from {}...'.format(urls[int(choice)].url))
        return int(choice)


def download(url, id, path=None, aws_access_key=None, aws_secret_key=None):
    """Download data from a DOS service."""
    local_client = Client(url)
    client = local_client.client
    data_object = client.GetDataObject(data_object_id=id).result().data_object
    # test to see what urls there are
    # check the url for s3 location
    # convert s3 location to container and key
    download_url = None
    if len(data_object.urls) != 0:
        download_url = data_object.urls[pick_url(
            data_object.urls, len(data_object.urls) - 1)]
    if path is None:
        path = data_object.id
    return handle_url(
        download_url,
        path,
        aws_secret_key=aws_secret_key,
        aws_access_key=aws_access_key)


def main(args=None):
    parser = argparse.ArgumentParser(
        description='Download files from a Data Object Service.')
    parser.add_argument('url',
                        help='The location of the Data Object Service')
    parser.add_argument('id',
                        help='The identifier of the Data Object')
    parser.add_argument('--aws_access_key', help="An AWS access key")
    parser.add_argument('--aws_secret_key', help='AWS Secret access key')
    parser.add_argument('--path', help='Download to the specified location, '
                                       'defaults to the Data Object id')
    args = parser.parse_args(args)
    download(
        args.url,
        args.id,
        path=args.path,
        aws_access_key=args.aws_access_key,
        aws_secret_key=args.aws_secret_key)


if __name__ == '__main__':
    main()

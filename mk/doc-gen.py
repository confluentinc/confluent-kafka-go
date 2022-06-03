#!/usr/bin/env python3

# Extract godoc HTML documentation for our packages,
# remove some nonsense, update some links and make it ready
# for inclusion in Confluent doc tree.


import subprocess
import re
import os
import time
import sys
from urllib.error import URLError
import urllib.request
from bs4 import BeautifulSoup


# Copy the content of an url to a directory and
# return its path using relative_path as base
def copy_to_static(url, dir, relative_path='static'):
    basename = url.split('/')[-1]
    file = urllib.request.urlopen(url).read()
    with open(f'{dir}/{basename}', 'wb') as f:
        f.write(file)
    return f'{relative_path}/{basename}'


if __name__ == '__main__':
    docs_path = 'docs'
    static_path = f'{docs_path}/static'
    serve_host = 'localhost:8080'
    # Use godoc client to extract our package docs
    print('Starting godoc server', file=sys.stderr)
    server = subprocess.Popen(['godoc', f'-http={serve_host}'])
    try:
        server.communicate('', timeout=0)
    except subprocess.TimeoutExpired:
        while True:
            try:
                urllib.request.urlopen(f'http://{serve_host}').read()
                # server is up
                break
            except URLError:
                time.sleep(0.1)

    try:
        html_in = subprocess.check_output(
            'godoc ' +
            '-url=/pkg/github.com/confluentinc/confluent-kafka-go/kafka' +
            ' | egrep -v "^using (GOPATH|module) mode"', shell=True)

        # Parse HTML
        soup = BeautifulSoup(html_in, 'html.parser')

        # Remove topbar (Blog, Search, etc)
        topbar = soup.find(id='topbar').decompose()

        # Remove "Subdirectories"
        soup.find(id='pkg-subdirectories').decompose()
        soup.find(attrs={'class': 'pkg-dir'}).decompose()
        for t in soup.find_all(href='#pkg-subdirectories'):
            t.decompose()

        os.makedirs(static_path, exist_ok=True)
        # Use golang.org for external resources (such as CSS and JS)
        for t in soup.find_all(href=re.compile(r'^/')):
            href = t['href']
            if href.endswith('.css'):
                print(f'Saving {href}', file=sys.stderr)
                new_href = copy_to_static(
                    f'http://{serve_host}{href}', static_path)
                t['href'] = new_href

        for t in soup.find_all(src=re.compile(r'^/')):
            src = t['src']
            if src.endswith('.js'):
                print(f'Saving {src}', file=sys.stderr)
                new_src = copy_to_static(
                    f'http://{serve_host}{src}', static_path)
                t['src'] = new_src

        # Write updated HTML to stdout
        print(soup.prettify())
    finally:
        print("Terminating server", file=sys.stderr)
        server.terminate()

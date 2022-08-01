#!/usr/bin/env python3

# Extract godoc HTML documentation for our packages,
# remove some nonsense, update some links and make it ready
# for inclusion in Confluent doc tree.


import subprocess
import re
import sys
from bs4 import BeautifulSoup


def convert_path(url, base_url, after):
    relative_path = url[url.rfind(after) + len(after):]
    if relative_path == "style.css":
        relative_path = "styles.css"
    return f'{base_url}/{relative_path}'


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <package>")
        sys.exit(1)
    package = sys.argv[1]

    tag = "v1.9.2"
    base_css = "https://go.dev/css"
    base_js = "https://go.dev/js"
    base_src = "https://github.com/confluentinc/" + \
               f"confluent-kafka-go/blob/{tag}"
    base_pkg = "https://pkg.go.dev"
    license = "https://go.dev/LICENSE"

    # Use godoc client to extract our package docs
    html_in = subprocess.check_output(
        'godoc -url=/pkg/github.com/confluentinc/' +
        f'confluent-kafka-go/{package} ' +
        '| egrep -v "^using (GOPATH|module) mode"', shell=True)

    # Parse HTML
    soup = BeautifulSoup(html_in, 'html.parser')

    # Remove topbar (Blog, Search, etc)
    topbar = soup.find(id='topbar').decompose()

    # Remove "Subdirectories"
    soup.find(id='pkg-subdirectories').decompose()
    soup.find(attrs={'class': 'pkg-dir'}).decompose()
    for t in soup.find_all(href='#pkg-subdirectories'):
        t.decompose()

    # Use golang.org for external resources (such as CSS and JS)
    # Use github.com for source files
    for t in soup.find_all(href=re.compile(r'^/')):
        href = t['href']
        if href.endswith(".css"):
            t['href'] = convert_path(href, base_css, "/")
        elif href.startswith("/src/"):
            t['href'] = convert_path(href, base_src, "/confluent-kafka-go/")
        elif href.startswith("/pkg/"):
            t['href'] = convert_path(href, base_pkg, "/pkg/")
        elif href == "/LICENSE":
            t['href'] = license

    for t in soup.find_all(src=re.compile(r'^/')):
        if t['src'].endswith(".js"):
            t['src'] = convert_path(t['src'], base_js, "/")

    # Write updated HTML to stdout
    print(soup.prettify())

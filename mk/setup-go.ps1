if (!(Test-Path -Path ($env:USERPROFILE + "\go"))) {
    echo "Downloading and installing Go 1.17"
    curl https://go.dev/dl/go1.17.13.windows-amd64.zip -o ($env:USERPROFILE + '\go.zip')
    tar -xf ($env:USERPROFILE + '\go.zip') -C $env:USERPROFILE
} else {
    echo "Using previously installed Go 1.17"
}

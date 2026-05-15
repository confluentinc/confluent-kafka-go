if (!(Test-Path -Path ($env:USERPROFILE + "\go"))) {
    echo "Downloading and installing Go 1.25.0"
    curl https://go.dev/dl/go1.25.0.windows-amd64.zip -o ($env:USERPROFILE + '\go.zip')
    tar -xf ($env:USERPROFILE + '\go.zip') -C $env:USERPROFILE
} else {
    echo "Using previously installed Go 1.25.0"
}

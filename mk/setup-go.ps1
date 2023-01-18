if (!(Test-Path -Path ($env:USERPROFILE + "\go"))) {
    echo "Downloading and installing Go 1.17"
    Set-Location $env:USERPROFILE
    curl https://go.dev/dl/go1.17.13.windows-amd64.zip -o go.zip
    tar -xf go.zip
} else {
    echo "Using previously installed Go 1.17"
}

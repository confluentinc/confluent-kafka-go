if (!(Test-Path -Path ($env:USERPROFILE + "\go"))) {
    echo "Downloading and installing Go 1.17"
    curl https://dl.google.com/go/go1.17.windows-amd64.msi -o go.msi
    msiexec /i go.msi TARGETDIR=($env:USERPROFILE + "\go") /quiet
} else {
    echo "Using previously installed Go 1.17"
}

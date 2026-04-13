# Install `chb`

The recommended way to install `chb` is from the GitHub Releases page. You do not need Go installed.

## One-line installer

```bash
curl -fsSL https://raw.githubusercontent.com/CommonsHub/chb/main/install.sh | bash
```

This installs the latest release for your Linux architecture.

## Linux

Pick the asset that matches your CPU:

- `chb_<version>_linux_amd64.tar.gz` for most Intel/AMD Linux machines
- `chb_<version>_linux_arm64.tar.gz` for ARM64 Linux machines

Example for Linux `amd64`:

```bash
VERSION=v2.2.0
ARCH=amd64
curl -L -o /tmp/chb.tar.gz "https://github.com/CommonsHub/chb/releases/download/${VERSION}/chb_${VERSION#v}_linux_${ARCH}.tar.gz"
tar -xzf /tmp/chb.tar.gz -C /tmp
install /tmp/chb_${VERSION#v}_linux_${ARCH} /usr/local/bin/chb
chb --version
```

Or with the installer script pinned to a specific release:

```bash
curl -fsSL https://raw.githubusercontent.com/CommonsHub/chb/main/install.sh | VERSION=v2.2.0 bash
```

Example for Linux `arm64`:

```bash
VERSION=v2.2.0
ARCH=arm64
curl -L -o /tmp/chb.tar.gz "https://github.com/CommonsHub/chb/releases/download/${VERSION}/chb_${VERSION#v}_linux_${ARCH}.tar.gz"
tar -xzf /tmp/chb.tar.gz -C /tmp
install /tmp/chb_${VERSION#v}_linux_${ARCH} /usr/local/bin/chb
chb --version
```

## Verify downloads

Each release also publishes `checksums.txt`.

```bash
VERSION=v2.2.0
curl -L -O "https://github.com/CommonsHub/chb/releases/download/${VERSION}/checksums.txt"
sha256sum -c checksums.txt --ignore-missing
```

## Developer install with Go

If you do want to build from source:

```bash
go install github.com/CommonsHub/chb@latest
```

Or:

```bash
git clone https://github.com/CommonsHub/chb.git
cd chb
go build -o chb .
```
